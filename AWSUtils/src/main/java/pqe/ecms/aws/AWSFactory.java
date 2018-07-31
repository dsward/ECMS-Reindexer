package pqe.ecms.aws;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

/**
 * This is the factory for all things AWS. </br>
 * This requires the System property "ecms.environment" to be set to one of nighty|preprod|platform-preprod|testing|prod
 */
public class AWSFactory {
	private static final ReentrantLock lock = new ReentrantLock();
	private static AWSCredentialsProvider credentials;
	private static AmazonS3 s3Client;
	private static AmazonSQS sqsClient;

	/**
	 * This creates the credentials chain that we support of looking for the ~/.aws/credentials first then falling back on the Instance credentials if the server is in AWS.
	 * @return
	 */
	private static AWSCredentialsProvider getCredentials() {
		if (null == credentials) {
			lock.lock();
			try {
				if (null == credentials)
					credentials = new AWSCredentialsProviderChain(new ProfileCredentialsProvider(System.getProperty("ecms.environment")), new InstanceProfileCredentialsProvider(false));
			} finally {
				lock.unlock();
			}
		}
		return credentials;
	}

	/**
	 * Retrieves the s3 properties using the ecms.environment system property.
	 * @return
	 * @throws IOException
	 */
	public static Properties getS3Properties() throws IOException {
		Properties prop = new Properties();
		prop.load(AWSFactory.class.getResourceAsStream("/" + System.getProperty("ecms.environment") + "/s3.properties"));
		return prop;
	}

	/**
	 * Retrieves the s3 client build with the credentials retrieved from {@link AWSFactory#getCredentials()}
	 * @return
	 */
	public static AmazonS3 getS3Client() {
		if (null == s3Client) {
			lock.lock();
			try {
				if (null == s3Client)
					s3Client = AmazonS3ClientBuilder.standard().withRegion("us-east-1").withCredentials(getCredentials()).build();
			} finally {
				lock.unlock();
			}
		}
		return s3Client;
	}

	/**
	 * Retrieves the SQS client build with the credentials retrieved from {@link AWSFactory#getCredentials()}
	 * @return
	 */
	public static AmazonSQS getSQSClient() {
		if (null == sqsClient) {
			lock.lock();
			try {
				if (null == sqsClient)
					sqsClient = AmazonSQSClientBuilder.standard().withRegion("us-east-1").withCredentials(getCredentials()).build();
			} finally {
				lock.unlock();
			}
		}
		return sqsClient;
	}
}
