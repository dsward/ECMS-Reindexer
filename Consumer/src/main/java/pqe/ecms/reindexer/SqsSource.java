package pqe.ecms.reindexer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.aws.AWSFactory;

import java.io.IOException;
import java.util.List;

public class SqsSource extends AbstractSource<Long> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqsSource.class);

	private ObjectMapper objectMapper = new ObjectMapper();

	private String queueUrl;
	private AmazonSQS sqsClient;

	private String lastReceiptHandle;

	public SqsSource(String queueUrl) {
		this.queueUrl = queueUrl;
		this.sqsClient = AWSFactory.getSQSClient();
	}

	@Override
	protected Logger logger() {
		return LOGGER;
	}

	public boolean next() {
		try {
			if (lastReceiptHandle != null) {
				DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
				deleteMessageRequest.setQueueUrl(queueUrl);
				deleteMessageRequest.setReceiptHandle(lastReceiptHandle);

				LOGGER.debug("Deleting SQS message {}", lastReceiptHandle);
				sqsClient.deleteMessage(deleteMessageRequest);

				lastReceiptHandle = null;
			}

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setMaxNumberOfMessages(1);
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setWaitTimeSeconds(10);

			List<Message> messageList = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
			if (messageList.isEmpty()) {
				LOGGER.info("No more messages in SQS queue {}", queueUrl);
				return false;
			}

			Message message = messageList.remove(0);
			lastReceiptHandle = message.getReceiptHandle();
			String body = message.getBody();

			LOGGER.debug("Received SQS message {}", message.getReceiptHandle());

			QueueMessage queueMessage = objectMapper.readValue(body, QueueMessage.class);
			obtrudeQueue(queueMessage.getDocumentIdList());

			return true;

		} catch (AmazonClientException e) {
			LOGGER.error("Exception receiving messages from SQS queue {}", queueUrl, e);

		} catch (IOException e) {
			LOGGER.error("Exception parsing message from SQS queue {}", queueUrl, e);
		}

		return false;
	}
}
