package com.pc.lambda;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.pc.lambda.model.Request;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

// https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/sqs/src/main/java/com/example/sqs/SQSExample.java

public class Handler implements RequestHandler<List<Request>, String> {

	private LambdaLogger logger = null;
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public String handleRequest(List<Request> requestJson, Context context) {

		logger = context.getLogger();

		sendMessages(requestJson);

		return "";
	}

	private void sendMessages(List<Request> requestJson) {
		logger.log("\nInside sendMessages method.");

		SortedMap<Integer, HashMap<String, List<SendMessageBatchRequestEntry>>> mapPriorityToQueueMessages = new TreeMap<Integer, HashMap<String, List<SendMessageBatchRequestEntry>>>();

		int batchMessageId = 0;
		// 1. Parse the Input JSON request.
		for (Request request : requestJson) {
			int priority = request.getPriority();
			logger.log("\nPriority: " + String.valueOf(priority));

			// 2. Find out the SQS queue URL based on the priority.
			String sqsQueueUrl = "";
			if (priority == 2) {
				sqsQueueUrl = System.getenv("twoHrTATQueue");
			} else if (priority == 6) {
				sqsQueueUrl = System.getenv("sixHrTATQueue");
			} else if (priority == 10) {
				sqsQueueUrl = System.getenv("tenHrTATQueue");
			} else if (priority == 16) {
				sqsQueueUrl = System.getenv("sixteenHrTATQueue");
			}

			if (sqsQueueUrl.isEmpty()) {
				logger.log("\n The queue url cannot be determined for request: " + gson.toJson(request)
						+ " with priority: " + priority);
				continue;
			}

			HashMap<String, List<SendMessageBatchRequestEntry>> mapQueueToMessages = null;
			if (mapPriorityToQueueMessages.containsKey(priority)) {
				mapQueueToMessages = mapPriorityToQueueMessages.get(priority);
			} else {
				mapQueueToMessages = new HashMap<String, List<SendMessageBatchRequestEntry>>();
			}

			List<SendMessageBatchRequestEntry> entries = null;
			if (mapQueueToMessages.containsKey(sqsQueueUrl)) {
				entries = mapQueueToMessages.get(sqsQueueUrl);
			} else {
				entries = new ArrayList<SendMessageBatchRequestEntry>();
				batchMessageId = 0;
			}

			SendMessageBatchRequestEntry entry = SendMessageBatchRequestEntry.builder()
					.id(String.valueOf(batchMessageId)).messageGroupId(String.valueOf(priority))
					.messageBody(gson.toJson(request)).build();
			entries.add(entry);
			mapQueueToMessages.put(sqsQueueUrl, entries);
			mapPriorityToQueueMessages.put(priority, mapQueueToMessages);

			batchMessageId++;

			// 3. Send the message to the appropriate queue.
//			SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
//			SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(sqsQueueUrl)
//					.messageGroupId(String.valueOf(priority)).messageBody(gson.toJson(request)).build();
//			SendMessageResponse sendMessageResponse = sqsClient.sendMessage(sendMessageRequest);
//
//			logger.log("\nMessage ID: " + sendMessageResponse.messageId());
		}

		for (Entry<Integer, HashMap<String, List<SendMessageBatchRequestEntry>>> mapEntry : mapPriorityToQueueMessages
				.entrySet()) {
			HashMap<String, List<SendMessageBatchRequestEntry>> mapQueueToMessages = mapEntry.getValue();
			for (String sqsQueueUrl : mapQueueToMessages.keySet()) {
				logger.log("\n====== Sending batch messges to SQS Queue URL: " + sqsQueueUrl + " with priority "
						+ mapEntry.getKey() + "started ======");

				try {
					List<SendMessageBatchRequestEntry> entries = mapQueueToMessages.get(sqsQueueUrl);

					// 3. Send the message to the appropriate queue.
					SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder().entries(entries)
							.queueUrl(sqsQueueUrl).build();
					SendMessageBatchResponse sendMessageBatchResponse = SqsClient.builder().region(Region.US_EAST_1)
							.build().sendMessageBatch(sendMessageBatchRequest);

					// 4. Log the successful messages.
					List<SendMessageBatchResultEntry> successEntries = sendMessageBatchResponse.successful();
					if (!successEntries.isEmpty()) {
						logger.log("\nSuccessful Entries: ");
						for (Iterator<SendMessageBatchResultEntry> iterator = successEntries.iterator(); iterator
								.hasNext();) {
							SendMessageBatchResultEntry sendMessageBatchResultEntry = (SendMessageBatchResultEntry) iterator
									.next();
							logger.log("\n" + sendMessageBatchResultEntry.toString());
						}
					}

					// 5. Log the failed messages.
					List<BatchResultErrorEntry> failedEntries = sendMessageBatchResponse.failed();
					if (!failedEntries.isEmpty()) {
						logger.log("\nFailed Entries: ");
						for (Iterator<BatchResultErrorEntry> iterator = failedEntries.iterator(); iterator.hasNext();) {
							BatchResultErrorEntry batchResultErrorEntry = (BatchResultErrorEntry) iterator.next();
							logger.log("\n" + batchResultErrorEntry.toString());
						}
					}
				} catch (Exception ex) {
					logger.log("\nException when peforming operations for queue: " + sqsQueueUrl + " .\nDetails: "
							+ ex.getStackTrace());
				}

				logger.log("\n====== Sending batch messges to SQS Queue URL: " + sqsQueueUrl + " with priority "
						+ mapEntry.getKey() + " ended ======");
			}
		}

		logger.log("\nsendMessages method completed successfully");
	}
}
