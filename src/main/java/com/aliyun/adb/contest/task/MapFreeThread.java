package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.common.Util;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author: wangkai
 * @create: 2021-08-19 21:56
 **/
public class MapFreeThread {


    public static final ConcurrentLinkedQueue<ByteBuffer> QUEUE = new ConcurrentLinkedQueue<>();

    public static void free(ByteBuffer buf) {
        QUEUE.offer(buf);
    }

    public static void start() {
        Thread thread = new Thread(() -> {
            try {
                for (; ; ) {
                    ByteBuffer buf = QUEUE.poll();
                    if (buf == null) {
                        Thread.sleep(1);
                        continue;
                    }
                    Util.free(buf);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "free-thread");
        thread.start();
    }

}
