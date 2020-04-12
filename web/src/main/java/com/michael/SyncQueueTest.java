package com.michael;

import java.util.concurrent.SynchronousQueue;

/**
 * @author Michael Chu
 * @since 2020-03-26 16:03
 */
public class SyncQueueTest {

    static SynchronousQueue synchronousQueue = new SynchronousQueue();

    public static void main(String[] args) throws Exception {
        Thread thread1 = new Thread(() -> handle());
        thread1.start();
        Thread thread2 = new Thread(() -> handle());
        thread2.start();
        synchronousQueue.take();
    }

    static void handle() {
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
