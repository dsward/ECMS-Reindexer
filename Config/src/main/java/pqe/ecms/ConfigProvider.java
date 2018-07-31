package pqe.ecms;

import java.io.InputStream;

/**
 * ConfigProvider defines the access model for a configuration by name. <br/>
 * Determines if it has access to available text content as defined by the config name. <br/>
 * Initiates & returns the InputStream for the text content.
 */
public interface ConfigProvider {
	InputStream getConfig(String configName);

	boolean isResolvable(String configName);
}
