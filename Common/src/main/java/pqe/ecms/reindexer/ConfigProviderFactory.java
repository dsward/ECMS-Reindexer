package pqe.ecms.reindexer;

import com.proquest.configuration.Configuration;
import com.proquest.configuration.NotFoundException;
import pqe.ecms.ConfigProvider;
import pqe.ecms.aws.config.S3ConfigProvider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConfigProviderFactory {

	/**
	 * Interprets the users <b>source</b> command line parameter into a {@link ConfigProvider}
	 *
	 * @param commandLine
	 * @return
	 * @throws NotFoundException
	 * @throws IOException
	 */
	public static ConfigProvider getProvider(Configuration commandLine) throws NotFoundException, IOException {
		return ConfigProviderFactory.getProvider(
				commandLine.keyExists("source") ? commandLine.getString("source") : "s3"
		);
	}

	public static ConfigProvider getProvider(String source) throws IOException {
		switch (source) {
			case "file":
				return new ConfigProvider() {
					@Override
					public InputStream getConfig(String configName) {
						try {
							return Files.newInputStream(Paths.get(System.getProperty("file.root") + configName));
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}

					@Override
					public boolean isResolvable(String configName) {
						return Paths.get(System.getProperty("file.root") + configName).toFile().exists();
					}
				};

			case "jar":
				return new ConfigProvider() {
					@Override
					public InputStream getConfig(String configName) {
						try {
							return ConfigProviderFactory.class.getResourceAsStream(configName);
						} catch (NullPointerException e) {
							throw new RuntimeException(new IOException("Failed to find the jar config " + configName));
						}
					}

					@Override
					public boolean isResolvable(String configName) {
						return ConfigProviderFactory.class.getResource(configName) != null;
					}
				};

			case "s3":
				return new S3ConfigProvider();

			default:
				throw new IOException("Could not find a provider for the config source " + source);
		}

	}
}
