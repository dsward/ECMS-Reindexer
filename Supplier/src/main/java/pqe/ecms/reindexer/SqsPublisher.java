package pqe.ecms.reindexer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.aws.AWSFactory;

import java.util.List;
import java.util.function.Consumer;

public class SqsPublisher implements Consumer<List<Long>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqsPublisher.class);

	private String queueUrl;
	private AmazonSQS sqsClient;

	private ObjectMapper objectMapper = new ObjectMapper();

	public SqsPublisher(String queueUrl) {
		this.queueUrl = queueUrl;
		this.sqsClient = AWSFactory.getSQSClient();
	}

	@Override
	public void accept(List<Long> documentIdList) {
		try {
			LOGGER.trace("Queuing list of {} document ids", documentIdList.size());

			String messageText = objectMapper.writeValueAsString(new QueueMessage(documentIdList));

			SendMessageRequest send_msg_request = new SendMessageRequest()
					.withQueueUrl(queueUrl)
					.withMessageBody(messageText)
					.withDelaySeconds(0);
			sqsClient.sendMessage(send_msg_request);

		} catch (JsonProcessingException e) {
			LOGGER.error("Exception creating message for queue", e);

		} catch (AmazonClientException e) {
			LOGGER.error("Exception queuing message", e);
		}
	}

}
