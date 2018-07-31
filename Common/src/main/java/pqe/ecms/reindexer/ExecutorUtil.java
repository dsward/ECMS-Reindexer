package pqe.ecms.reindexer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorUtil {

	protected static void shutdownExecutorService(ExecutorService service) {
		if (!service.isShutdown()) {
			service.shutdown();
			while (!service.isTerminated()) {
				try {
					service.awaitTermination(10, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
				}
			}
		}
	}

}
