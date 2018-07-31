package pqe.ecms.reindexer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proquest.configuration.Configuration;
import com.proquest.configuration.ConfigurationLoader;
import com.proquest.editorial.commons.properties.PropertyResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.ConfigProvider;
import pqe.ecms.metrics.Gatherer;
import pqe.ecms.reindexer.sql.SqlSource;

import java.util.Properties;
import java.util.concurrent.*;

public class ReindexingSupplier {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReindexingSupplier.class);

	private static final String CONFIG = "command_line";

	public static void main(String[] args) {
		try {
			Gatherer.getInstance();

			ReindexingSupplier application = new ReindexingSupplier();
			application.execute(args);

		} catch (Throwable t) {
			LOGGER.error("Exiting due to Throwable", t);

		} finally {
			Gatherer.getInstance().logTimedActions();
		}
	}

	private void execute(String[] args) throws Exception {
		Configuration commandLine = ConfigurationLoader.loadXMLConfig(CONFIG);
		commandLine.parseArguments(args);
		System.setProperty("ecms.environment", commandLine.getString("instance"));

		ConfigProvider jarProvider = ConfigProviderFactory.getProvider("jar");

		Properties sqlProperties = PropertyResources.readPropertiesFromJar("/" + System.getProperty("ecms.environment") + "/sql.properties", ReindexingSupplier.class);
		SqlConfig sqlConfig = new SqlConfig(sqlProperties);

		AbstractSource<Long> source = new SqlSource(sqlConfig);
		SqsPublisher sqsPublisher = new SqsPublisher("https://sqs.us-east-1.amazonaws.com/365859773477/ecms-" + System.getProperty("ecms.environment") + "-indexing");

		ThreadFactory threadFactory = new ThreadFactoryBuilder().build();
		BlockingQueue<Runnable> sqsQueue = new ArrayBlockingQueue<>(100);
		ExecutorService sqsService = new ThreadPoolExecutor(1, 10, 0L, TimeUnit.MILLISECONDS, sqsQueue, threadFactory,
				(r, executor) -> {
					if (!executor.isShutdown()) {
						try {
							executor.getQueue().put(r);
						} catch (InterruptedException e) {
							;
						}
					}
				});

		QueueBatching<Long> queueBatching = new QueueBatching<>(100, batch -> sqsService.submit(() -> sqsPublisher.accept(batch)));

		queueBatching.indexContent(source.stream());
		ExecutorUtil.shutdownExecutorService(sqsService);

		LOGGER.info("Queued {} document ids in {} batches", queueBatching.getItemsCount(), queueBatching.getBatchCount());
	}

}
