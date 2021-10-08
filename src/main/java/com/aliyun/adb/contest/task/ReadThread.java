package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.SimpleAnalyticDB;
import com.aliyun.adb.contest.Table;
import com.aliyun.adb.contest.common.Util;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.aliyun.adb.contest.common.Constants.*;

/**
 * @author: wangkai
 * @create: 2021-06-26 12:51
 **/
public class ReadThread extends Thread {

    public static final ReadThread[] READ_THREADS = new ReadThread[READ_THREAD_SIZE];

    public static final Queue<ReadTask> free_read_tasks = new ConcurrentLinkedQueue<>();

    public final int index;
    public Table table;

    public long read_pos;
    public long read_limit;

    public ReadThread(int index) {
        super("Read-Thread-" + index);
        this.index = index;
    }

    private void read(
            FileChannel channel, ReadTask read_task, long read_pos, int read_size) throws IOException {
        MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, read_pos, read_size);
        map.load();
        read_task.read_buf = map;
    }

    public static void init_threads() {
        for (int i = 0; i < READ_THREADS.length; i++) {
            READ_THREADS[i] = new ReadThread(i);
        }
    }

    public static void start_threads() {
        for (int i = 0; i < READ_THREAD_SIZE; i++) {
            init_read_parse_tasks();
            READ_THREADS[i].table = SimpleAnalyticDB.get_table(i, READ_THREAD_SIZE);
            READ_THREADS[i].start();
        }
    }

    private static void init_read_parse_tasks() {
        for (int i = 0; i < READ_TASK_COUNT; i++) {
            free_read_tasks.offer(new ReadTask(false));
            ParseThread.free_parse_tasks.offer(new ParseTask(false));
            ParseThread.free_parse_tasks.offer(new ParseTask(false));
            ParseThread.free_parse_tasks.offer(new ParseTask(false));
        }
    }

    @Override
    public void run() {
        try {
            Table table = this.table;
            Queue<ReadTask> free_read_tasks = ReadThread.free_read_tasks;
            Queue<ReadTask> work_tasks = ParseThread.work_read_tasks;
            FileChannel channel = Util.open_read_only(table.table_file_path);

            ReadTask read_task = null;
            long read_pos = this.read_pos;
            long read_limit = this.read_limit;
            long soft_read_limit = Math.min(table.file_size, read_limit + READ_BUF_PADDING);

            for (; ; ) {
                read_task = Util.take_read_task(free_read_tasks);
                if (read_task == null) {
                    continue;
                }
                int read_size = (int) Math.min(soft_read_limit - read_pos, SOFT_READ_BUF_SIZE);
                read(channel, read_task, read_pos, read_size);
                read_task.read_pos = read_pos;
                read_task.table = table;
                if (read_size == SOFT_READ_BUF_SIZE) {
                    read_pos += READ_BUF_SIZE;
                } else {
                    read_pos += read_size;
                }
                boolean is_final_block = read_pos >= read_limit;
                read_task.read_size = read_size;
                work_tasks.offer(read_task);
                if (is_final_block) {
                    break;
                }
            }
            SimpleAnalyticDB.read_waiter.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
