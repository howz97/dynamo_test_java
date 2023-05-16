package org.example;

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

public class AsyncWorker extends Worker {
    private static final int num_clients = 2;
    private static final int pool_size = 3;
    private static DynamoDbAsyncClient clients[];
    private DynamoDbAsyncClient client;
    private ReentrantLock mu = new ReentrantLock();
    private Condition cv = mu.newCondition();
    private int inflight = 0;
    private final int maxInflight;

    AsyncWorker(int id, int maxf) {
        client = clients[id % clients.length];
        maxInflight = maxf;
    }

    public void run() {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            return;
        }

        while (!exited()) {
            mu.lock();
            inflight++;
            mu.unlock();

            GetItemRequest request = randRequest();
            CompletableFuture<GetItemResponse> fut = client.getItem(request);
            fut.whenComplete((resp, err) -> {
                try {
                    assert (resp.hasItem());
                    updateCounter();
                } catch (DynamoDbException e) {
                    System.err.println(e.getMessage());
                    killed.set(true);
                } finally {
                    mu.lock();
                    inflight--;
                    cv.signal();
                    mu.unlock();
                }
            });

            mu.lock();
            try {
                if (inflight == maxInflight) {
                    cv.await();
                }
            } catch (InterruptedException excetion) {
                Thread.currentThread().interrupt(); // set interrupted flag
            } finally {
                mu.unlock();
            }
        }

        mu.lock();
        try {
            while (inflight > 0) {
                cv.await();
            }
        } catch (InterruptedException excetion) {
            Thread.currentThread().interrupt();
        } finally {
            mu.unlock();
        }
    }

    static void InitClients() {
        AsyncWorker.clients = new DynamoDbAsyncClient[num_clients];
        for (int i = 0; i < num_clients; ++i) {
            AsyncWorker.clients[i] = DynamoDbAsyncClient.builder()
                    .region(Region.AP_NORTHEAST_1).asyncConfiguration(
                            b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                                    Runnable::run))
                    .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                            .eventLoopGroupBuilder(SdkEventLoopGroup.builder().numberOfThreads(pool_size)))
                    .build();
        }
    }
}
