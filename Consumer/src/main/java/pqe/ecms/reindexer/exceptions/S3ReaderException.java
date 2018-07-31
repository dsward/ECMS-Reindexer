package pqe.ecms.reindexer.exceptions;

import com.amazonaws.AmazonClientException;

public class S3ReaderException extends Throwable {
	public S3ReaderException(AmazonClientException e) {
		super(e);
	}
}
