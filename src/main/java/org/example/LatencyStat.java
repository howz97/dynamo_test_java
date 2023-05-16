package org.example;

import java.util.concurrent.atomic.AtomicInteger;

public class LatencyStat {
    private final static int bucket_size = 5;
    private final static int bucket_num = 1000;
    private AtomicInteger buckets[];
    private AtomicInteger sum = new AtomicInteger(0);

    LatencyStat() {
        buckets = new AtomicInteger[bucket_num];
        for (int i = 0; i < bucket_num; ++i) {
            buckets[i] = new AtomicInteger(0);
        }
    }

    void Record(int latency) {
        sum.addAndGet(latency);
        if (latency >= bucket_size * bucket_num) {
            buckets[bucket_num - 1].incrementAndGet();
        } else {
            buckets[latency / bucket_size].incrementAndGet();
        }
    };

    int LatencyPercentiles(double p) {
        assert (p <= 1);
        int cnt = TotalCount();
        int left = cnt;
        int i = bucket_num - 1;
        for (; i >= 0; --i) {
            if ((double) (left - buckets[i].get()) / (double) (cnt) >= p) {
                left -= buckets[i].get();
            } else {
                break;
            }
        }
        return (i + 1) * bucket_size;
    }

    int AverageLatency() {
        int cnt = TotalCount();
        if (cnt == 0) {
            return 0;
        }
        return sum.get() / cnt;
    }

    int TotalCount() {
        int cnt = 0;
        for (int i = 0; i < bucket_size; ++i) {
            cnt += buckets[i].get();
        }
        return cnt;
    }
}
