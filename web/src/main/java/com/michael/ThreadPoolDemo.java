package com.michael;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Michael Chu
 * @since 2020-03-26 14:58
 */
public class ThreadPoolDemo {

    public static void main(String[] args) {
        AtomicInteger integer = new AtomicInteger();
        int i = integer.incrementAndGet();
        System.out.println(i);
    }
}
