package org.example;

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.regions.Region;

import java.util.concurrent.BrokenBarrierException;

public class SyncWorker extends Worker {
    private final DynamoDbClient client = DynamoDbClient.builder()
            .region(Region.AP_NORTHEAST_1)
            .build();
    private int task_left;

    SyncWorker(int tasks) {
        task_left = tasks;
    }

    public void run() {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            return;
        }

        while (!exited() && task_left > 0) {
            try {
                GetItemRequest request = randRequest();
                long start = System.currentTimeMillis();
                GetItemResponse resp = client.getItem(request);
                stat.Record((int) (System.currentTimeMillis() - start));
                assert (resp.hasItem());
                updateCounter();
                task_left--;
            } catch (DynamoDbException e) {
                System.err.println(e.getMessage());
                killed.set(true);
            }
        }
    }
}
