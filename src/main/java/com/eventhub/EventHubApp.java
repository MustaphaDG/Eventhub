package com.eventhub;

import com.azure.core.util.IterableStream;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class EventHubApp {
    private static final String ENV_CONNECTION_STRING = "EVENTHUB_CONNECTION_STRING";
    private static final String ENV_EVENTHUB_NAME = "EVENTHUB_NAME";
    private static final String ENV_CONSUMER_GROUP = "EVENTHUB_CONSUMER_GROUP";
    private static final String ENV_PARTITION_ID = "EVENTHUB_PARTITION_ID";
    private static final int DEFAULT_MAX_EVENTS = 10;

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0].toLowerCase(Locale.ROOT);
        switch (mode) {
            case "send":
                sendEvents(Arrays.copyOfRange(args, 1, args.length));
                break;
            case "receive":
                String partitionId = args.length > 1 ? args[1] : null;
                receiveEvents(partitionId);
                break;
            default:
                System.err.println("Unknown mode: " + args[0]);
                printUsage();
        }
    }

    private static void sendEvents(String[] payloads) {
        List<String> messages = payloads.length > 0 ? Arrays.asList(payloads) : List.of("Hello Event Hubs!");
        EventHubClientBuilder builder = buildClientBuilder();

        try (EventHubProducerClient producer = builder.buildProducerClient()) {
            EventDataBatch batch = producer.createBatch();
            int sentCount = 0;

            for (String message : messages) {
                EventData eventData = new EventData(message);
                if (!batch.tryAdd(eventData)) {
                    producer.send(batch);
                    sentCount += batch.getCount();
                    batch = producer.createBatch();
                    if (!batch.tryAdd(eventData)) {
                        throw new IllegalArgumentException("Event payload is too large for an empty batch.");
                    }
                }
            }

            if (batch.getCount() > 0) {
                producer.send(batch);
                sentCount += batch.getCount();
            }

            System.out.println("Sent " + sentCount + " event(s) to Event Hubs.");
        }
    }

    private static void receiveEvents(String partitionIdOverride) {
        EventHubClientBuilder builder = buildClientBuilder();
        String consumerGroup = getEnvOrDefault(ENV_CONSUMER_GROUP, EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME);

        try (EventHubConsumerClient consumer = builder.consumerGroup(consumerGroup).buildConsumerClient()) {
            String partitionId = resolvePartitionId(consumer, partitionIdOverride);
            Iterable<PartitionEvent> events = consumer.receiveFromPartition(
                partitionId,
                DEFAULT_MAX_EVENTS,
                EventPosition.earliest(),
                Duration.ofSeconds(30)
            );

            int receivedCount = 0;
            for (PartitionEvent event : events) {
                receivedCount++;
                String body = event.getData().getBodyAsString();
                System.out.println("Received event from partition " + partitionId + ": " + body);
            }

            System.out.println("Received " + receivedCount + " event(s).");
        }
    }

    private static EventHubClientBuilder buildClientBuilder() {
        String connectionString = requireEnv(ENV_CONNECTION_STRING);
        String eventHubName = System.getenv(ENV_EVENTHUB_NAME);

        if (eventHubName == null || eventHubName.isBlank()) {
            if (!hasEntityPath(connectionString)) {
                throw new IllegalArgumentException(
                    "EVENTHUB_NAME is required when the connection string does not include EntityPath."
                );
            }
            return new EventHubClientBuilder().connectionString(connectionString);
        }

        return new EventHubClientBuilder().connectionString(connectionString, eventHubName);
    }

    private static String resolvePartitionId(EventHubConsumerClient consumer, String partitionOverride) {
        String resolved = partitionOverride;
        if (resolved == null || resolved.isBlank()) {
            resolved = getEnvOrDefault(ENV_PARTITION_ID, null);
        }
        if (resolved == null || resolved.isBlank()) {
            IterableStream<String> partitionIds = consumer.getPartitionIds();
            java.util.Iterator<String> iterator = partitionIds.iterator();
            if (!iterator.hasNext()) {
                throw new IllegalStateException("No partitions were found for the Event Hub.");
            }
            resolved = iterator.next();
        }
        return resolved;
    }

    static boolean hasEntityPath(String connectionString) {
        String[] segments = connectionString.split(";");
        for (String segment : segments) {
            int equalsIndex = segment.indexOf('=');
            if (equalsIndex <= 0) {
                continue;
            }
            String key = segment.substring(0, equalsIndex).trim();
            if ("EntityPath".equalsIgnoreCase(key)) {
                return true;
            }
        }
        return false;
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: " + name);
        }
        return value;
    }

    private static String getEnvOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar target/eventhub-app-1.0-SNAPSHOT.jar send [message1 message2 ...]");
        System.out.println("  java -jar target/eventhub-app-1.0-SNAPSHOT.jar receive [partitionId]");
        System.out.println();
        System.out.println("Environment variables:");
        System.out.println("  EVENTHUB_CONNECTION_STRING (required)");
        System.out.println("  EVENTHUB_NAME (required if connection string has no EntityPath)");
        System.out.println("  EVENTHUB_CONSUMER_GROUP (optional, defaults to $Default)");
        System.out.println("  EVENTHUB_PARTITION_ID (optional, defaults to first partition)");
    }
}
