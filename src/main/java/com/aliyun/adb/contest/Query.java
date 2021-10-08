package com.aliyun.adb.contest;

import com.aliyun.adb.contest.common.ByteBuf;
import com.aliyun.adb.contest.common.Constants;
import com.aliyun.adb.contest.common.SortArray;
import com.aliyun.adb.contest.task.SortTask;
import com.aliyun.adb.contest.task.SortThread;

import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: wangkai
 * @create: 2021-07-02 23:28
 **/
public class Query extends Constants {

    static final boolean QUEUE_CONDITION = true;

    static final AtomicInteger auto = new AtomicInteger();

    public int query_time;
    public final BlockingQueue<Query> queue;
    public final AtomicInteger task_condition;

    public final int index = auto.getAndIncrement();
    public final SortTask sort_task_2;
    public final SortTask sort_task_3;
    public final SortThread sort_thread_2;
    public final SortThread sort_thread_3;
    public final FileChannel[] channels_2;
    public final FileChannel[] channels_3;
    public final FileChannel[] channels_1 = new FileChannel[BUCKET_COUNT * 4];
    public final SortArray sort_array_1 = new SortArray();
    public final ByteBuf read_buf = ByteBuf.allocate(QUERY_READ_BUF_SIZE / SORT_THREAD_SIZE);

    public Query() {
        if (SORT_THREAD_SIZE == 3) {
            channels_2 = new FileChannel[BUCKET_COUNT * 4];
            sort_task_2 = new SortTask(this);
            sort_thread_2 = new SortThread(index);
            sort_thread_2.start();

            channels_3 = new FileChannel[BUCKET_COUNT * 4];
            sort_task_3 = new SortTask(this);
            sort_thread_3 = new SortThread(index);
            sort_thread_3.start();
        } else {
            channels_2 = new FileChannel[BUCKET_COUNT * 4];
            sort_task_2 = new SortTask(this);
            sort_thread_2 = new SortThread(index);
            sort_thread_2.start();

            channels_3 = null;
            sort_task_3 = null;
            sort_thread_3 = null;
        }
        if (QUEUE_CONDITION) {
            queue = new ArrayBlockingQueue<>(4);
            task_condition = null;
        } else {
            task_condition = new AtomicInteger();
            queue = null;
        }
    }

    public void begin_task() {
        if (QUEUE_CONDITION) {

        } else {
            task_condition.set(1);
        }
    }

    public void wait_task_finish() throws InterruptedException {
        if (QUEUE_CONDITION) {
            queue.poll(1, TimeUnit.SECONDS);
        } else {
            AtomicInteger task_condition = this.task_condition;
            for (; ; ) {
                int count = task_condition.get();
                if (count == SORT_THREAD_SIZE) {
                    break;
                } else {
                    Thread.sleep(1);
                }
            }
        }
    }

    public void finish_task() {
        if (QUEUE_CONDITION) {
            queue.offer(this);
        } else {
            task_condition.getAndIncrement();
        }
    }

}
