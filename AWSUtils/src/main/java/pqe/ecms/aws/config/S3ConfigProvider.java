package pqe.ecms.aws.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import pqe.ecms.ConfigProvider;
import pqe.ecms.aws.AWSFactory;

/**
 * This {@link ConfigProvider} resolves configurations that exist in S3. <br/>
 * The S3 bucket & root is defined in the internal s3.properties file.
 */
public class S3ConfigProvider implements ConfigProvider {
	private static final Logger LOG = LoggerFactory.getLogger(S3ConfigProvider.class);
	private static final Properties S3_INFORMATION;
	static {
		try {
			S3_INFORMATION = AWSFactory.getS3Properties();
		} catch (IOException e) {
			LOG.error("Failed to load internal S3 properties: " + "/" + System.getProperty("ecms.environment") + "/s3.properties", e);
			throw new RuntimeException(e);
		}
	}

	private String resolveKey(String configName) {
		return S3_INFORMATION.getProperty("ecms.s3.config.root") + configName;
	}

	@Override
	public InputStream getConfig(String configName) {
		AmazonS3 client = AWSFactory.getS3Client();
		S3Object config = client.getObject(S3_INFORMATION.getProperty("ecms.s3.bucket"), resolveKey(configName));
		return config.getObjectContent();
	}

	@Override
	public boolean isResolvable(String configName) {
		AmazonS3 client = AWSFactory.getS3Client();
		return client.doesObjectExist(S3_INFORMATION.getProperty("ecms.s3.bucket"), resolveKey(configName));
	}
}
