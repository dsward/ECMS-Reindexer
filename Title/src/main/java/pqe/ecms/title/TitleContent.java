package pqe.ecms.title;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.client.EcmsClientFactory;
import pqe.ecms.client.exception.ResourceNotFoundException;
import pqe.ecms.client.exception.ServiceClientException;
import pqe.ecms.client.exception.ServiceContainerException;
import pqe.ecms.client.exception.ServiceResponseException;
import pqe.ecms.client.titlemanagement.TitleManagementClient;
import pqe.ecms.client.titlemanagement.domain.PublicationId;
import pqe.ecms.client.titlemanagement.domain.TitleInformation;
import pqe.ecms.title.exceptions.TitleContentException;
import pqe.ecms.title.exceptions.TitleNotFoundException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Handles the retrieval and caching of title-related information from the Title Management Service.<br>
 */
public class TitleContent {

	private static final Logger LOGGER = LoggerFactory.getLogger(TitleContent.class);

	private static final Integer MAX_CACHED_TITLES = 1000;
	private static final int CACHE_DURATION_VALUE = 10;
	private static final TimeUnit CACHE_DURATION_UNIT = TimeUnit.MINUTES;

	private static final int MAX_ATTEMPTS = 7; // Not an evidence-based value
	public static final int BASIC_RETRY_SLEEP_INTERVAL_MS = 100;

	private TitleManagementClient titleManagementClient;

	private LoadingCache<PublicationId, TitleInformation> titleInfoCache;

	public TitleContent(EcmsClientFactory ecmsClientFactory) {
		this.titleManagementClient = ecmsClientFactory.getClient(TitleManagementClient.class);
		titleInfoCache = buildTitleContentCache();
	}

	/**
	 * Retrieves the title information associated with the given title id (i.e. CBLID) <br>
	 *
	 * @param pubId The id of the title for which to fetch metadata
	 * @return The full TitleInformation object for the title
	 * @throws TitleContentException If an error occurred in the retrieval of the data for the title.
	 */
	public TitleInformation getTitleInfo(final PublicationId pubId) throws TitleContentException {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Retrieving title information for title id <{}>", pubId.toString());
		}

		return fetchFromCache(pubId, titleInfoCache, "title information");
	}

	@FunctionalInterface
	public interface CacheSupplier<T, U> {
		U apply(T input) throws ExecutionException, UncheckedExecutionException, ExecutionError;
	}

	private <T> T fetchFromCache(final PublicationId pubId, LoadingCache<PublicationId, T> cache, String description) throws TitleContentException {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Retrieving {} for title <{}>", description, pubId.toString());
		}

		try {
			return cache.get(pubId);

		} catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
			if (e.getCause() != null) {
				if (e.getCause() instanceof TitleContentException) {
					throw (TitleContentException) e.getCause();
				}
			}
			throw new TitleContentException(String.format("Could not retrieve %s for title <%s>", description, pubId.toString()), e);

		} finally {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Finished process of retrieving {} for title <{}>", description, pubId.toString());
			}
		}
	}

	/**
	 * Creates the cache and the loader process for titleInfoContent into the cache.
	 *
	 * @return A cache configured for retrieving title information from the title management service.
	 */
	private LoadingCache<PublicationId, TitleInformation> buildTitleContentCache() {
		return CacheBuilder.newBuilder()
				.maximumSize(MAX_CACHED_TITLES)
				.expireAfterWrite(CACHE_DURATION_VALUE, CACHE_DURATION_UNIT)
				.build(new CacheLoader<PublicationId, TitleInformation>() {

					public TitleInformation load(PublicationId pubId) throws Exception {
						return fetchTitleInfo(pubId);
					}
				});
	}

	@FunctionalInterface
	public interface TitleManagementFetchSupplier<T> {

		T get() throws ResourceNotFoundException, ServiceClientException, ServiceContainerException, ServiceResponseException;
	}

	private <T> T fetchFromTitleManagement(PublicationId pubId, TitleManagementFetchSupplier<T> fetchSupplier) throws Exception {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Loading cache with data for <{}>", pubId.toString());
		}

		for (int attemptNumber = 0; attemptNumber < MAX_ATTEMPTS; attemptNumber++) {
			try {
				return fetchSupplier.get();

			} catch (ResourceNotFoundException e) {
				LOGGER.warn("Fatal exception for (attempt {} of {}) exception fetching title info <{}>", attemptNumber + 1, MAX_ATTEMPTS, pubId.toString());
				throw new TitleNotFoundException(e);

			} catch (ServiceClientException | ServiceContainerException e) {
				if (attemptNumber + 1 == MAX_ATTEMPTS) {
					LOGGER.warn("Out of retries (attempt {} of {}) for exception fetching title info <{}>", attemptNumber + 1, MAX_ATTEMPTS, pubId.toString(), e);
					throw new TitleContentException(e);
				}

				LOGGER.warn("Retry needed (attempt {} of {}) for exception fetching title info <{}>", attemptNumber + 1, MAX_ATTEMPTS, pubId.toString(), e);
				try {
					Thread.sleep(BASIC_RETRY_SLEEP_INTERVAL_MS);
				} catch (InterruptedException e1) {
					// Whatever.
				}

			} catch (ServiceResponseException e) {
				if (attemptNumber + 1 == MAX_ATTEMPTS) {
					LOGGER.warn("Out of retries (attempt {} of {}) for response exception fetching title info <{}>, code={}", attemptNumber + 1, MAX_ATTEMPTS, pubId.toString(), e.code());
					throw new TitleContentException(e);
				}

				LOGGER.warn("Retry needed (attempt {} of {}) for response exception fetching title info <{}>, code={}", attemptNumber + 1, MAX_ATTEMPTS, pubId.toString(), e.code());
				try {
					Thread.sleep(BASIC_RETRY_SLEEP_INTERVAL_MS * (attemptNumber + 1));
				} catch (InterruptedException e1) {
					// Whatever.
				}
			}
		}

		throw new TitleContentException();
	}

	private TitleInformation fetchTitleInfo(PublicationId pubId) throws Exception {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Loading cache with data for <{}>", pubId.toString());
		}

		return fetchFromTitleManagement(pubId, () ->
				titleManagementClient.getTitleInfo(pubId)
		);
	}

}
