package org.example;

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import software.amazon.awssdk.regions.Region;

public class SyncWorker extends Worker {
    private final DynamoDbClient client = DynamoDbClient.builder()
            .region(Region.AP_NORTHEAST_1).endpointOverride(endpoint)
            .build();

    public void run() {
        while (!exited()) {
            GetItemRequest request = randRequest();
            try {
                GetItemResponse resp = client.getItem(request);
                assert (resp.hasItem());
                updateCounter();
            } catch (DynamoDbException e) {
                System.err.println(e.getMessage());
                killed.set(true);
            }
        }
    }
}
