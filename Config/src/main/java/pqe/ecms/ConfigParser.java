package pqe.ecms;

import java.io.InputStream;

/**
 * Parses an {@link InputStream} into the T object.
 * @param <T>
 */
public interface ConfigParser<T> {
	T parseConfig(InputStream configStream);
}
