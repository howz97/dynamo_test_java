package org.example;

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncWorker extends Worker {
    private final static DynamoDbAsyncClient client = DynamoDbAsyncClient.builder()
            .region(Region.AP_NORTHEAST_1).endpointOverride(endpoint).asyncConfiguration(
                    b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                            Runnable::run))
            .build();
    private AtomicInteger inflight = new AtomicInteger(0);
    private final int maxInflight;

    AsyncWorker(int maxf) {
        maxInflight = maxf;
    }

    public void run() {
        while (!killed.get()) {
            inflight.incrementAndGet();

            GetItemRequest request = randRequest();
            CompletableFuture<GetItemResponse> fut = client.getItem(request);
            fut.whenComplete((resp, err) -> {
                try {
                    if (resp.hasItem()) {
                        count.incrementAndGet();
                    } else {
                        System.err.println("item not exists");
                        killed.set(true);
                    }
                } catch (DynamoDbException e) {
                    System.err.println(e.getMessage());
                    killed.set(true);
                } finally {
                    inflight.decrementAndGet();
                }
            });

            while (inflight.get() == maxInflight) {
            }
        }
        while (inflight.get() > 0) {
        }
    }
}
