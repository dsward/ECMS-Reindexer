package pqe.ecms.reindexer;

import java.util.Properties;

public class SqlConfig {
	private String username;
	private String password;
	private String url;
	private Direction direction;

	public SqlConfig(Properties sqlProperties) {
		this.username = sqlProperties.getProperty("db.username", sqlProperties.getProperty("javax.persistence.jdbc.user"));
		this.password = sqlProperties.getProperty("db.password", sqlProperties.getProperty("javax.persistence.jdbc.password"));
		this.url = sqlProperties.getProperty("db.url", sqlProperties.getProperty("javax.persistence.jdbc.url"));
		this.direction = Direction.DESC;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public static enum Direction {
		ASC,
		DESC;
	}

	public Direction getDirection() {
		return direction;
	}

	public void setDirection(Direction direction) {
		this.direction = direction;
	}
}
