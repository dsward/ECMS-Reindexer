package pqe.ecms.reindexer;

import com.proquest.editorial.commons.properties.PropertyResources;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.reindexer.exceptions.ClientInstantiationException;

import java.util.Properties;

/**
 * Solr utilities to create solr connections.
 */
public class SolrFactory {

	private static final Logger LOGGER = LoggerFactory.getLogger(SolrFactory.class);

	private static final String KEY_SOLR_ZOOKEEPER_URL = "solr.zookeeper.url";
	private static final String KEY_SOLR_CONNECTION_TIMEOUT = "solr.connectiontimeout";
	private static final String KEY_SOLR_CLIENT_TIMEOUT = "solr.clienttimeout";
	private static final SolrClientSingleton solrClientSingleton = new SolrClientSingleton();

	/**
	 * Returns a singleton solr client connection.
	 *
	 * @return A properly configured solr client connection ready to accept solr commands.
	 */
	public static CloudSolrClient getCloudSolrClient() {
		try {
			return solrClientSingleton.get();
		} catch (ConcurrentException e) {
			final StringBuilder msg = new StringBuilder("Unable to instantiate the cloud solr client: ").append(e.getMessage());
			throw new ClientInstantiationException(msg.toString(), e);
		}
	}

	/**
	 * Handles the instantiation of the CloudSolrClient connection and the returning of the singleton.
	 *
	 * @author fperez
	 */
	private static class SolrClientSingleton extends LazyInitializer<CloudSolrClient> {

		@Override
		protected CloudSolrClient initialize() throws ConcurrentException {
			final Properties solrProperties = PropertyResources.getNamedProperties("solr");

			CloudSolrClient solrClient;

			final String zookeeperUrl = solrProperties.getProperty(KEY_SOLR_ZOOKEEPER_URL);
			final String connectionTimeout = solrProperties.getProperty(KEY_SOLR_CONNECTION_TIMEOUT);
			final String clientTimeout = solrProperties.getProperty(KEY_SOLR_CLIENT_TIMEOUT);

			if (zookeeperUrl == null || connectionTimeout == null || clientTimeout == null) {
				final String msg = "Missing one or more of required values in solr properties:  zookeeperUrl|connectionTimeout|clientTimeout";
				throw new IllegalArgumentException(msg);
			}

			int zkConnTimeout = Integer.parseInt(connectionTimeout);
			int zkClientTimeout = Integer.parseInt(clientTimeout);

			solrClient = new CloudSolrClient(zookeeperUrl);
			solrClient.setZkConnectTimeout(zkConnTimeout);
			solrClient.setZkClientTimeout(zkClientTimeout);

			return solrClient;
		}

	}

}
