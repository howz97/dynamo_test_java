package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;

import java.util.Vector;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final int run_seconds = 30;
    private static final int report_interval = 5;
    private static final int num_clients = 2;
    private static final int pool_size = 2;

    public static void main(String... args) {
        String cmd = args[0];
        int num_thds = Integer.parseInt(args[1]);
        Vector<Worker> workers = new Vector<Worker>(num_thds);
        if (cmd.equals("sync")) {
            for (int i = 0; i < num_thds; ++i) {
                SyncWorker w = new SyncWorker();
                w.start();
                workers.add(w);
            }
        } else if (cmd.equals("async")) {
            int req_per_thd = Integer.parseInt(args[2]);
            AsyncWorker.clients = new DynamoDbAsyncClient[num_clients];
            for (int i = 0; i < num_clients; ++i) {
                AsyncWorker.clients[i] = DynamoDbAsyncClient.builder()
                        .region(Region.AP_NORTHEAST_1).endpointOverride(Worker.endpoint).asyncConfiguration(
                                b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                                        Runnable::run))
                        .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                                .eventLoopGroupBuilder(SdkEventLoopGroup.builder().numberOfThreads(pool_size)))
                        .build();
            }
            for (int i = 0; i < num_thds; ++i) {
                AsyncWorker w = new AsyncWorker(i, req_per_thd);
                w.start();
                workers.add(w);
            }
        } else {
            logger.error("unknown command {}", cmd);
            System.exit(1);
        }
        try {
            int last_v = 0;
            for (int i = 0; i < run_seconds; i += report_interval) {
                Thread.sleep(report_interval * 1000);
                int qps = (Worker.count.get() - last_v) / report_interval;
                logger.info("qps {}", qps);
                last_v = Worker.count.get();
            }
            Worker.killed.set(true);
            for (Worker w : workers) {
                w.join();
            }
            int qps = Worker.count.get() / run_seconds;
            logger.info("qps average {}", qps);
        } catch (InterruptedException e) {
            logger.error("error {}", e);
        }

        System.exit(0);
    }
}
