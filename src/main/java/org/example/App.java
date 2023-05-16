package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String... args) {
        String cmd = args[0];
        int num_thds = Integer.parseInt(args[1]);
        Vector<Worker> workers = new Vector<Worker>(num_thds);
        Worker.initBarrier(num_thds);
        if (cmd.equals("sync")) {
            int tasks_size = (Worker.test_size / num_thds) + 1;
            for (int i = 0; i < num_thds; ++i) {
                SyncWorker w = new SyncWorker(tasks_size);
                w.start();
                workers.add(w);
            }
        } else if (cmd.equals("async")) {
            int maxf = Integer.parseInt(args[2]);
            AsyncWorker.InitClients();
            for (int i = 0; i < num_thds; ++i) {
                AsyncWorker w = new AsyncWorker(i, maxf);
                w.start();
                workers.add(w);
            }
        } else {
            logger.error("unknown command {}", cmd);
            System.exit(1);
        }
        try {
            logger.info("threads ready...");
            Worker.setStartTs();
            Worker.barrier.await();
            for (Worker w : workers) {
                w.join();
            }
            int p95 = Worker.stat.LatencyPercentiles(0.95);
            logger.info("latency: average={} 95%={}", Worker.stat.AverageLatency(), p95);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        System.exit(0);
    }
}
