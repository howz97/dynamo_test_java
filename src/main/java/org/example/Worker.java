package org.example;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.net.URI;
import java.util.HashMap;

public class Worker extends Thread {
    private static final int test_size = 100000;
    private static final int report_interval = 10000;
    private static AtomicInteger counter = new AtomicInteger(0);
    private static AtomicLong last_report_ts = new AtomicLong(System.currentTimeMillis());
    protected static AtomicBoolean killed = new AtomicBoolean(false);
    protected final static URI endpoint = URI.create("http://127.0.0.1:8050");
    private final static String tableName = "test.rand";
    private final static int tableSize = 1000;
    private final static boolean consistentRead = false;
    private static Random rand = new Random();
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    protected GetItemRequest randRequest() {
        HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();
        keyToGet.put("id", AttributeValue.builder().n(String.valueOf(rand.nextInt(tableSize))).build());
        GetItemRequest request = GetItemRequest.builder()
                .key(keyToGet)
                .tableName(tableName).consistentRead(consistentRead)
                .build();
        return request;
    }

    protected void updateCounter() {
        int cnt = counter.incrementAndGet();
        if (cnt % report_interval == 0) {
            long cur_ts = System.currentTimeMillis();
            long last_ts = last_report_ts.getAndSet(cur_ts);
            if (cur_ts > last_ts) {
                int cost_ms = (int) (cur_ts - last_ts);
                int qps = report_interval * 1000 / cost_ms;
                logger.info("counter {}, cost {}ms, qps {}", cnt, cost_ms, qps);
            } else {
                logger.info("counter {}, conflict error detected", cnt);
            }
        }
    }

    protected boolean exited() {
        return counter.get() >= test_size || killed.get();
    }
}
