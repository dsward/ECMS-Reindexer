package pqe.ecms.title;


import pqe.ecms.client.EcmsClientFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the singleton TitleContent to leverage its caching.
 */
public class TitleContentFactory {

	private static Map<EcmsClientFactory, TitleContent> titleContentMap = new ConcurrentHashMap<>();
	private static final Object lock = new Object();

	/**
	 * Get the singleton TitleContent
	 *
	 * @return The singleton
	 */
	public static TitleContent getInstance(EcmsClientFactory ecmsClientFactory) {
		if (!titleContentMap.containsKey(ecmsClientFactory)) {
			synchronized (lock) {
				titleContentMap.computeIfAbsent(ecmsClientFactory, TitleContent::new);
			}
		}
		return titleContentMap.get(ecmsClientFactory);
	}

	/**
	 * Set the instance of TitleContent the factory should return.
	 *
	 * @param titleContent The new singleton TitleContent
	 */
	public static void setInstance(EcmsClientFactory ecmsClientFactory, TitleContent titleContent) {
		titleContentMap.put(ecmsClientFactory, titleContent);
	}
}
