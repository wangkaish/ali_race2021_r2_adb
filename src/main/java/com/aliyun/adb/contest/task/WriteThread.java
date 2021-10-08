package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.SimpleAnalyticDB;
import com.aliyun.adb.contest.common.Constants;
import com.aliyun.adb.contest.common.Util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author: wangkai
 * @create: 2021-06-30 21:46
 **/
public class WriteThread extends Thread {

    public static final WriteThread[] WRITE_THREADS = new WriteThread[Constants.WRITE_THREAD_SIZE];

    public static final Queue<WriteTask> free_write_tasks = new ConcurrentLinkedQueue<>();
    public static final Queue<WriteTask> work_write_tasks = new ConcurrentLinkedQueue<>();

    private final int index;

    public WriteThread(int index) {
        super("Write-Thread-" + index);
        this.index = index;
    }

    public static void start_threads() {
        for (int i = 0; i < Constants.WRITE_THREAD_SIZE; i++) {
            WRITE_THREADS[i] = new WriteThread(i);
            WRITE_THREADS[i].start();
        }
    }

    public static void finish() {
        for (int i = 0; i < Constants.WRITE_THREAD_SIZE; i++) {
            work_write_tasks.offer(new WriteTask());
        }
    }

    @Override
    public void run() {
        try {
            Queue<WriteTask> free_write_tasks = WriteThread.free_write_tasks;
            Queue<WriteTask> work_write_tasks = WriteThread.work_write_tasks;
            for (; ; ) {
                WriteTask task = Util.take_write_task(work_write_tasks);
                if (task.is_finally_block) {
                    SimpleAnalyticDB.write_waiter.countDown();
                    break;
                }
                task.write();
                free_write_tasks.offer(task);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
