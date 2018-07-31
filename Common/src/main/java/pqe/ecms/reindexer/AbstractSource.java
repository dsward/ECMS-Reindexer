package pqe.ecms.reindexer;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Search supplier executes the search(es) that are supplied through configuration streaming the resulting document identifiers. <br/>
 * An {@link ArrayBlockingQueue} is used to buffer the SOLR searching against the utilization of the items in the {@link Stream}.
 * This queue is used to populate the {@link Stream} through the use of  a custom queue spliterator {@link QueuedSpliterator}
 */
public abstract class AbstractSource<T> implements Consumer<T> {

	private ArrayBlockingQueue<T> pending;
	private Stream<T> outputStream;
	private QueuedSpliterator spliterator;
	private LongAdder count;

	protected AbstractSource() {
		this.pending = new ArrayBlockingQueue<>(500);
		this.spliterator = new QueuedSpliterator(pending);
		this.outputStream = StreamSupport.stream(spliterator, false);
		this.count = new LongAdder();
	}

	public void obtrudeQueue(Collection<T> values) {
		pending = new ArrayBlockingQueue<>(values.size());
		pending.addAll(values);
		count.add(values.size());

		spliterator = new QueuedSpliterator(pending);
		outputStream = StreamSupport.stream(spliterator, false);

		spliterator.complete();
	}

	protected void waitUntilEmpty() {
		if (pending.size() > 0) {
			logger().info("Waiting for the source queue to empty");
			while (pending.size() > 0) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
			logger().info("The source queue has been emptied");
		} else {
			logger().info("The source queue is empty");
		}
		spliterator.complete();
	}

	protected abstract Logger logger();

	/**
	 * Returns the {@link Stream} of the search results.  This is the only way to access the content being asynchronously fed into the queue.
	 *
	 * @return
	 */
	public Stream<T> stream() {
		return outputStream;
	}

	/**
	 * Pushes identifiers onto the queue that feeds the stream.
	 *
	 * @param value
	 */
	@Override
	public void accept(T value) {
		try {
			pending.put(value);
			count.increment();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Things are going wrong while queuing. (accept)", e);
		}
	}

	/**
	 * This spliterator provides the ability to create the stream out of the queue provided by the search supplier.
	 * Because the source of the queue is an asynchronous SOLR feed the spliterator needs to be told when there is no more content.
	 */
	private class QueuedSpliterator implements Spliterator<T> {
		private final BlockingQueue<T> queue;
		private boolean hasMoreContent;

		public QueuedSpliterator(BlockingQueue<T> queue) {
			this.queue = queue;
			hasMoreContent = true;
		}

		/**
		 * Allows the external source of the content being fed into the queue to notify the spliterator that there will be no more content provided.
		 */
		public void complete() {
			hasMoreContent = false;
		}

		/**
		 * Pulls from the queue to add to the stream.
		 *
		 * @param action
		 * @return
		 */
		@Override
		public boolean tryAdvance(Consumer<? super T> action) {
			try {
				while (queue.size() > 0 || hasMoreContent) {
					T result = queue.poll(100L, TimeUnit.MILLISECONDS);
					if (null != result) {
						action.accept(result);
						return queue.size() > 0 || hasMoreContent;
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Things are going wrong while streaming. (tryAdvance)", e);
			}
			return hasMoreContent;
		}

		/**
		 * Takes the current contents of the queue as a separate stream.
		 *
		 * @return
		 */
		@Override
		public Spliterator<T> trySplit() {
			try {
				List<T> splitSet = new ArrayList<>(queue.size() + 1);
				splitSet.add(queue.take());
				queue.drainTo(splitSet);
				return splitSet.spliterator();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Things are going wrong while streaming. (trySplit)", e);
			}
		}

		@Override
		public long estimateSize() {
			return count.longValue();
		}

		@Override
		public int characteristics() {
			return Spliterator.CONCURRENT;
		}
	}
}
