package pqe.ecms;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * ConfigResolver is used to fetch and read into an object the specified configuration allowing the user to provider thei own {@link ConfigProvider} and/or {@link ConfigParser}
 */
public class ConfigResolver {
	/**
	 * Reads a config using the first {@link ConfigProvider} that resolves it and using the provide {@link ConfigParser}
	 * @param configName
	 * @param parser
	 * @param providers
	 * @param <T>
	 * @return
	 * @throws IOException
	 */
	public static <T> T loadConfig(String configName, ConfigParser<T> parser, ConfigProvider... providers) throws IOException {
		return parser.parseConfig(Stream.of(providers).filter(prov -> prov.isResolvable(configName)).findFirst().orElseThrow(() -> new IOException()).getConfig(configName));
	}

	/**
	 * Reads a config using the first {@link ConfigProvider} that resolves it and using an {@link ObjectMapper} to be the {@link ConfigParser}
	 * @param config
	 * @param configName
	 * @param providers
	 * @param <T>
	 * @return
	 * @throws IOException
	 */
	public static <T> T loadConfig(Class<T> config, String configName, ConfigProvider... providers) throws IOException {
		try {
			return loadConfig(configName, configStream -> {
				ObjectMapper mapper = new ObjectMapper();
				try {
					return mapper.readValue(configStream, config);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}, providers);
		} catch (RuntimeException e) {
			if (e.getCause() instanceof IOException) {
				throw (IOException) e.getCause();
			}
			throw e;
		}
	}
}
