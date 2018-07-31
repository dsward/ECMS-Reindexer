package pqe.ecms.reindexer.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.reindexer.AbstractSource;
import pqe.ecms.reindexer.SqlConfig;

import java.util.concurrent.CompletableFuture;

public class SqlSource extends AbstractSource<Long> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlSource.class);

	public SqlSource(SqlConfig config) {
		super();
		CompletableFuture.runAsync(new SqlExecutor(config, this) {
			@Override
			public void run() {
				super.run();
				SqlSource.this.waitUntilEmpty();
			}
		});
	}

	@Override
	protected Logger logger() {
		return LOGGER;
	}
}
