package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

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
            AsyncWorker.InitClients();
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
            for (Worker w : workers) {
                w.join();
            }
        } catch (InterruptedException e) {
            logger.error("error {}", e);
        }

        System.exit(0);
    }
}
