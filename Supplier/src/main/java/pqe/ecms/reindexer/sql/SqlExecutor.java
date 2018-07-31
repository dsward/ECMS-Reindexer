package pqe.ecms.reindexer.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.reindexer.SqlConfig;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

public class SqlExecutor implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlExecutor.class);

	private final SqlConfig config;
	private final Consumer<Long> consumer;

	public SqlExecutor(SqlConfig config, Consumer<Long> consumer) {
		this.config = config;
		this.consumer = consumer;
	}

	@Override
	public void run() {
		try (Connection connection = getConnection(config);
		     PreparedStatement statement = prepareStatement(config, connection);
		     ResultSet resultSet = executeQuery(statement)) {

			LOGGER.info("Retrieving ResultSet ...");
			while (resultSet.next()) {
				Long documentId = resultSet.getLong(1);
				consumer.accept(documentId);
			}
			LOGGER.info("Completed retrieval of ResultSet.");

		} catch (SQLException e) {
			LOGGER.error("Exception", e);
		}
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

	private PreparedStatement prepareStatement(SqlConfig config, Connection connection) throws SQLException {
		Instant startTime = Instant.now();
		String query;

		switch (config.getDirection()) {
			case ASC:
				query = "SELECT d.document_pk FROM documents.documents d ORDER BY d.document_pk ASC";
				break;

			default:
				query = "SELECT d.document_pk FROM documents.documents d ORDER BY d.document_pk DESC";
				break;
		}

		PreparedStatement statement = connection.prepareStatement(
				query,
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY
		);
		statement.setFetchSize(Integer.MIN_VALUE);

		LOGGER.debug("Prepared query statement in {}", Duration.between(startTime, Instant.now()));
		return statement;
	}

	private ResultSet executeQuery(PreparedStatement statement) throws SQLException {
		Instant startTime = Instant.now();

		LOGGER.debug("Executing query");
		ResultSet resultSet = statement.executeQuery();
		LOGGER.debug("Query executed in {}", Duration.between(startTime, Instant.now()));

		return resultSet;
	}

}
