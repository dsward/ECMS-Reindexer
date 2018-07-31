package pqe.ecms.reindexer.workflow;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.aws.AWSFactory;
import pqe.ecms.aws.AmazonS3Helper;
import pqe.ecms.reindexer.exceptions.WorkflowException;

import java.io.IOException;

public class S3Reader {

	private static final Logger LOGGER = LoggerFactory.getLogger(S3Reader.class);

	private String instance;

	public S3Reader(String instance) {
		this.instance = instance;
	}

	public IndexingDocument apply(IndexingDocument document) throws WorkflowException {
		if (document.getS3Partition() == null || document.getDocumentId() == null || document.getDocumentVersionId() == null) {
			LOGGER.error("Null document metadata: {}", document.toString());
			throw new WorkflowException();
		}

		GetObjectRequest getObjectRequest = new GetObjectRequest(
				document.getBucket(),
				document.getS3Partition().replaceAll("/", "_") + "/" + instance + "/" + document.getDocumentId().toString() + "/" + document.getDocumentVersionId().toString() + ".xml"
		);

		LOGGER.debug("GET key=" + getObjectRequest.getKey());
		AmazonS3 s3Client = AWSFactory.getS3Client();

		try {
			S3Object s3Object = s3Client.getObject(getObjectRequest);
			String text = AmazonS3Helper.readLinesFromAmazon(s3Object);

			document.getDocument().getBody().setContents(text);

		} catch (AmazonClientException e) {
			if (e instanceof AmazonS3Exception) {
				switch (((AmazonS3Exception) e).getStatusCode()) {
					case 404:
						LOGGER.warn("Key not found: {} {}", getObjectRequest.getBucketName(), getObjectRequest.getKey());

					default:
						LOGGER.error("Exception", e);
				}
			}
			throw new WorkflowException(e);

		} catch (IOException e) {
			LOGGER.error("Exception", e);
			throw new WorkflowException(e);
		}

		return document;
	}

}
