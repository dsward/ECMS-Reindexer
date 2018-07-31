package pqe.ecms.reindexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class QueueBatching<T> {
	private final static Logger LOGGER = LoggerFactory.getLogger(QueueBatching.class);

	private final Integer batchSize;
	private final Consumer<List<T>> batchSubmitter;
	private final LongAdder batchCount;
	private final LongAdder itemCount;

	public QueueBatching(Integer batchSize, Consumer<List<T>> batchSubmitter) {
		this.batchSize = batchSize;
		this.batchSubmitter = batchSubmitter;
		this.batchCount = new LongAdder();
		this.itemCount = new LongAdder();
	}

	public void indexContent(Stream<T> content) {
		content.collect(new BatchCollector<>(batchSize, (batch) -> {
			int size = batch.size();
			batchSubmitter.accept(batch);
			batchCount.increment();
			itemCount.add(size);
		}));
	}

	public int getBatchCount() {
		return batchCount.intValue();
	}

	public long getItemsCount() {
		return itemCount.longValue();
	}

	/**
	 * This implementation of a {@link Collector} does not return any content.  It instead collects the objects into batches of content determined by an external;y provided batchSize number. <br/>
	 * Each batch is then sent to the provided batchReceiver that implements a {@link Consumer} of the list of items. <br/>
	 * <br/>
	 * <h1>Usage:</h1>
	 * <code>
	 * Stream.collect(new BatchCollector<>(200, (batch) -> {});
	 * </code>
	 *
	 * @param <U>
	 */
	private class BatchCollector<U> implements Collector<U, List<U>, List<U>> {
		private final Consumer<List<U>> batchReceiver;
		private Integer batchSize;

		public BatchCollector(Integer batchSize, Consumer<List<U>> batchReceiver) {
			this.batchSize = batchSize;
			this.batchReceiver = batchReceiver;
		}

		@Override
		public Supplier<List<U>> supplier() {
			return ArrayList::new;
		}

		@Override
		public BiConsumer<List<U>, U> accumulator() {
			return (ts, t) -> {
				ts.add(t);
				if (ts.size() >= batchSize) {
					synchronized (batchReceiver) {
						// Going to not be thread safe process.
						List<U> batch = new ArrayList<>(ts);
						batchReceiver.accept(batch);
						ts.clear();
					}
				}
			};
		}

		@Override
		public BinaryOperator<List<U>> combiner() {
			return (ts, ots) -> {
				synchronized (batchReceiver) {
					List<U> batch = new ArrayList<>(ts);
					batch.addAll(ots);
					batchReceiver.accept(batch);
					ts.clear();
					ots.clear();
					return Collections.emptyList();
				}
			};
		}

		@Override
		public Function<List<U>, List<U>> finisher() {
			return ts -> {
				synchronized (batchReceiver) {
					List<U> batch = new ArrayList<>(ts);
					batchReceiver.accept(batch);
					ts.clear();
					return Collections.emptyList();
				}
			};
		}

		@Override
		public Set<Characteristics> characteristics() {
			return Collections.emptySet();
		}
	}
}
