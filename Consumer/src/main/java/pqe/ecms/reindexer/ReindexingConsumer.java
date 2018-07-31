package pqe.ecms.reindexer;

import com.proquest.configuration.Configuration;
import com.proquest.configuration.ConfigurationLoader;
import com.proquest.editorial.commons.properties.PropertyResources;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.ConfigProvider;
import pqe.ecms.metrics.Gatherer;
import pqe.ecms.reindexer.exceptions.WorkflowException;
import pqe.ecms.reindexer.workflow.DocumentMapper;
import pqe.ecms.reindexer.workflow.IndexingDocument;
import pqe.ecms.reindexer.workflow.S3Reader;
import pqe.ecms.reindexer.workflow.SqlReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class ReindexingConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReindexingConsumer.class);

	private static final String CONFIG = "command_line";

	public static void main(String[] args) {
		try {
			Gatherer.getInstance();

			ReindexingConsumer application = new ReindexingConsumer();
			application.execute(args);

		} catch (Throwable t) {
			LOGGER.error("Exiting due to Throwable", t);

		} finally {
			Gatherer.getInstance().logTimedActions();
		}
	}

	private SqlReader sqlReader;
	private S3Reader s3Reader;
	private DocumentMapper documentMapper;
	private CloudSolrClient solrClient;

	private Map<String, LongAdder> workflowCounts = new LinkedHashMap<>();

	public void execute(String[] args) throws Exception {
		LOGGER.info("Starting ReindexingConsumer");

		Configuration commandLine = ConfigurationLoader.loadXMLConfig(CONFIG);
		commandLine.parseArguments(args);
		System.setProperty("ecms.environment", commandLine.getString("instance"));

		ConfigProvider jarProvider = ConfigProviderFactory.getProvider("jar");

		initProperties("ecmsResource", "/global/ecmsResource.properties");
		initProperties("ecmsservices", "/" + System.getProperty("ecms.environment") + "/services.properties");
		initProperties("jpa", "/" + System.getProperty("ecms.environment") + "/jpaBase.properties");
		initProperties("solr", "/" + System.getProperty("ecms.environment") + "/solr.properties");

		SqsSource source = new SqsSource("https://sqs.us-east-1.amazonaws.com/365859773477/ecms-" + System.getProperty("ecms.environment") + "-indexing");

		SqlConfig sqlConfig = new SqlConfig(PropertyResources.getNamedProperties("jpa"));
		sqlReader = new SqlReader(sqlConfig);
		s3Reader = new S3Reader(System.getProperty("ecms.environment"));
		documentMapper = new DocumentMapper();
		solrClient = SolrFactory.getCloudSolrClient();

		ScheduledExecutorService execService = Executors.newScheduledThreadPool(1);
		execService.scheduleAtFixedRate(() -> {
			LOGGER.info("Status: {}",
					workflowCounts.entrySet().stream()
							.map(entry -> String.format("%s=%d", entry.getKey(), entry.getValue().intValue()))
							.collect(Collectors.joining(", "))
			);
		}, 5, 15, TimeUnit.SECONDS);

		while (source.next()) {
			List<Long> documentIds = source.stream().collect(Collectors.toList());
			LOGGER.info("Consumed {} documentIds", documentIds.size());

			List<SolrInputDocument> solrBatch = documentIds.stream()
					.map(this::getNewDocument)
					.map(this::fetchMetadata)
					.filter(Optional::isPresent)
					.map(Optional::get)
					.map(this::fetchDocumentBody)
					.filter(Optional::isPresent)
					.map(Optional::get)
					.map(this::mapDocumentForSolr)
					.filter(Optional::isPresent)
					.map(Optional::get)
					.map(this::toSolrInputDocument)
					.filter(Optional::isPresent)
					.map(Optional::get)
					.collect(Collectors.toList());

			try {
				LOGGER.info("Posting {} documents to Solr", solrBatch.size());
				solrClient.add("ecms", solrBatch);
			} catch (SolrServerException | IOException e) {
				LOGGER.error("Exception", e);
			}
		}

		execService.shutdown();
	}

	private void initProperties(String name, String path) throws IOException {
		Properties props = PropertyResources.readPropertiesFromJar(path, ReindexingConsumer.class);
		PropertyResources.loadNamedProperties(name, props);
	}

	private void count(String workflow) {
		workflowCounts.computeIfAbsent(workflow, x -> new LongAdder()).increment();
	}

	private IndexingDocument getNewDocument(Long documentId) {
		count("getNewDocument");
		IndexingDocument doc = new IndexingDocument();
		doc.setDocumentId(documentId);
		return doc;
	}

	private Optional<IndexingDocument> fetchMetadata(IndexingDocument document) {
		count("fetchMetadata");
		try {
			return Optional.of(sqlReader.apply(document));
		} catch (WorkflowException e) {
			LOGGER.warn("Exception reading document from SQL: {}", document.getDocumentId(), e);
			return Optional.empty();
		}
	}

	private Optional<IndexingDocument> fetchDocumentBody(IndexingDocument document) {
		count("fetchDocumentBody");
		try {
			return Optional.of(s3Reader.apply(document));
		} catch (WorkflowException e) {
			LOGGER.warn("Exception reading document from S3: {}", document.getDocumentId(), e);
			return Optional.empty();
		}
	}

	private Optional<IndexingDocument> mapDocumentForSolr(IndexingDocument document) {
		count("mapDocumentForSolr");
		try {
			return Optional.of(documentMapper.apply(document));
		} catch (WorkflowException e) {
			LOGGER.warn("Exception mapping document for Solr: {}", document.getDocumentId(), e);
			return Optional.empty();
		}
	}

	private Optional<SolrInputDocument> toSolrInputDocument(IndexingDocument document) {
		count("toSolrInputDocument");
		SolrInputDocument solrDoc = new SolrInputDocument();
		for (String field : document.getMappedDocument().fieldSet()) {
			for (String value : document.getMappedDocument().getFieldValues(field)) {
				solrDoc.addField(field, value);
			}
		}
		return Optional.of(solrDoc);
	}

}
