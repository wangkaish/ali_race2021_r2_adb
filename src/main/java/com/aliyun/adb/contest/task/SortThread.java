package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.common.ByteBuf;
import com.aliyun.adb.contest.common.SortArray;

import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author: wangkai
 * @create: 2021-07-11 10:54
 **/
public class SortThread extends Thread {

    public final BlockingQueue<SortTask> work_tasks = new ArrayBlockingQueue<>(1);

    public final int index;

    public SortThread(int index) {
        super("Sort-Thread-" + index);
//        setDaemon(true);
        this.index = index;
    }

    @Override
    public void run() {
        try {
            for (; ; ) {
                SortTask task = work_tasks.take();
                FileChannel channel = task.channel;
                SortArray sort_array = task.sort_array;
                ByteBuf read_buf = task.read_buf;
                int read_size = task.read_size;
                long read_pos = task.read_pos;
                sort_array.write(channel, read_pos, read_size, read_buf);
                task.query.finish_task();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
