package pqe.ecms.reindexer.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.reindexer.SqlConfig;
import pqe.ecms.reindexer.exceptions.WorkflowException;
import pqe.ecms.rest.message.document.Body;
import pqe.ecms.rest.message.document.Document;
import pqe.ecms.rest.message.document.Metadata;
import pqe.ecms.rest.message.document.externalid.DocumentIdentifierMetadata;
import pqe.ecms.rest.message.document.externalid.ExternalId;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SqlReader {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlReader.class);

	private static final String SELECT_DOCUMENT = "" +
			"SELECT " +
			"  d.create_date, " +
			"  lp.legacy_platform, " +
			"  d.legacy_id, " +
			"  v.document_version_pk, " +
			"  v.version as \"version\", " +
			"  v.date as \"version_date\", " +
			"  v.importdate_date as \"import_date\", " +
			"  v.client, " +
			"  v.user, " +
			"  v.exportable, " +
			"  s.schemaName, " +
			"  s.schemaVersion, " +
			"  b.bucket, " +
			"  d.s3_partition " +
			"FROM documents.documents d " +
			"JOIN documents.document_versions v ON d.document_pk = v.document_fk " +
			"JOIN documents.document_schemas s ON v.schema_fk = s.schema_id " +
			"JOIN documents.legacy_platforms lp ON d.lp_id = lp.lp_id " +
			"JOIN documents.buckets b ON d.bucket_fk = b.bucket_id " +
			"WHERE d.document_pk = ? " +
			"ORDER BY v.version DESC";

	private static final String SELECT_EXTERNALIDS = "" +
			"SELECT " +
			"  di.docid_platform, " +
			"  di.docid_value, " +
			"  di.docid_date " +
			"FROM documents.documents d " +
			"JOIN documents.document_identifiers di ON d.document_pk = di.document_fk " +
			"WHERE d.document_pk = ? ";

	private final SqlConfig sqlConfig;

	public SqlReader(SqlConfig sqlConfig) {
		this.sqlConfig = sqlConfig;
	}

	public IndexingDocument apply(IndexingDocument document) throws WorkflowException {
		Long id = document.getDocumentId();

		Metadata metadata = new Metadata();
		Body body = new Body();

		Document ecmsDoc = new Document();
		document.setDocument(ecmsDoc);
		document.getDocument().setMetadata(metadata);
		document.getDocument().setBody(body);

		// Fetch from SQL via JOINs

		try (Connection connection = getConnection(sqlConfig)) {
			try (PreparedStatement statement = connection.prepareStatement(SELECT_DOCUMENT);
			     ResultSet resultSet = executeForDocument(statement, document.getDocumentId())) {
				if (resultSet.next()) {
					document.setBucket(resultSet.getString("bucket"));
					document.setS3Partition(resultSet.getString("s3_partition"));
					document.setDocumentVersionId(resultSet.getLong("document_version_pk"));

					metadata.setExportable(resultSet.getBoolean("exportable"));
					metadata.setVersion(resultSet.getInt("version"));
					metadata.setLegacyId(resultSet.getString("legacy_id"));
					metadata.setLegacyPlatform(resultSet.getString("legacy_platform"));
					metadata.setClient(resultSet.getString("client"));
					metadata.setCreatedDate(resultSet.getTimestamp("create_date").toInstant().atZone(ZoneId.of("UTC")));
					metadata.setImportDate(resultSet.getTimestamp("import_date").toInstant().atZone(ZoneId.of("UTC")));
					metadata.setLastUpdateDate(resultSet.getTimestamp("version_date").toInstant().atZone(ZoneId.of("UTC")));
					metadata.setLastUpdateUserId(resultSet.getString("user"));
					metadata.setLastUpdateUserName(resultSet.getString("user"));

					document.getDocument().setVersion(resultSet.getInt("version"));
					document.getDocument().setVersionBy(resultSet.getString("user"));
					document.getDocument().setVersionDate(Date.from(metadata.getLastUpdateDate().toInstant()));

					body.setSchema(resultSet.getString("schemaName"));
					body.setVersion(resultSet.getString("schemaVersion"));
				}
			}

			ExternalId externalId = new ExternalId();
			List<DocumentIdentifierMetadata> idsList = new ArrayList<>();
			externalId.setDocumentIdentifierMetadata(idsList);
			metadata.setExternalIds(externalId);

			try (PreparedStatement statement = connection.prepareStatement(SELECT_EXTERNALIDS);
			     ResultSet resultSet = executeForDocument(statement, document.getDocumentId())) {
				while (resultSet.next()) {
					idsList.add(DocumentIdentifierMetadata.newBuilder()
							.identifierDate(Date.from(resultSet.getTimestamp("docid_date").toInstant().atZone(ZoneId.of("UTC")).toInstant()))
							.identifierPlatform(resultSet.getString("docid_platform"))
							.identifierValue(resultSet.getString("docid_value"))
							.build());
				}
			}
		} catch (SQLException e) {
			LOGGER.error("Exception", e);
			throw new WorkflowException(e);
		}

		return document;
	}

	private ResultSet executeForDocument(PreparedStatement statement, Long documentId) throws SQLException {
		statement.setLong(1, documentId);
		return statement.executeQuery();
	}

	/**
	 * Creates a new connection using jpa properties if a cached connection isn't available
	 *
	 * @return Connection
	 * @throws SQLException - Unable to create connection
	 */
	private Connection getConnection(SqlConfig config) throws SQLException {
		Instant startTime = Instant.now();
		LOGGER.debug("Acquiring SQL connection: {}@{}", config.getUsername(), config.getPassword());
		Connection connection = DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
		LOGGER.debug("Connected in {}", Duration.between(startTime, Instant.now()));
		return connection;
	}

}
