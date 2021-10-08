package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.SimpleAnalyticDB;
import com.aliyun.adb.contest.common.Constants;
import com.aliyun.adb.contest.common.LongUtil;
import com.aliyun.adb.contest.common.Util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.aliyun.adb.contest.common.Constants.*;
import static com.aliyun.adb.contest.common.Util.*;

/**
 * @author: wangkai
 * @create: 2021-06-05 22:34
 **/
public class ParseThread extends Thread {

    public static final ParseThread[] PARSE_THREADS = new ParseThread[PARSE_THREAD_SIZE];
    public static final Queue<ParseTask> free_parse_tasks = new ConcurrentLinkedQueue<>();
    public static final Queue<ReadTask> work_read_tasks = new ConcurrentLinkedQueue<>();

    public final int index;

    public ParseThread(int index) {
        super("Parse-Thread-" + index);
        this.index = index;
    }

    public static void start_threads() {
        for (int i = 0; i < PARSE_THREAD_SIZE; i++) {
            PARSE_THREADS[i] = new ParseThread(i);
            PARSE_THREADS[i].start();
        }
    }

    public static void finish() {
        for (int i = 0; i < PARSE_THREAD_SIZE; i++) {
            work_read_tasks.offer(new ReadTask(true));
        }
    }

    @Override
    public void run() {
        try {
            Queue<ReadTask> free_read_tasks = ReadThread.free_read_tasks;
            Queue<ReadTask> work_read_tasks = ParseThread.work_read_tasks;
            Queue<ParseTask> free_parse_tasks = ParseThread.free_parse_tasks;

            int i;
            int read_size;
            int scan_index = 0;
            int scan_end;
            long value1;
            long value2;
            long read_array;
            ReadTask read_task;
            ParseTask parse_task;
            for (; ; ) {
                read_task = Util.take_read_task(work_read_tasks);
                if (read_task.is_final_block) {
                    break;
                }

                parse_task = Util.take_parse_task(free_parse_tasks);

                long order_write_index = parse_task.order_buf.offset;
                long part_write_index = parse_task.part_buf.offset;

                read_size = read_task.read_size;
                read_array = Util.get_address(read_task.read_buf);
                if (read_task.read_pos == 0) {
                    scan_index = SimpleAnalyticDB.line_item.get_header_size() - 1;
                } else {
                    scan_index = 0;
                }
                scan_index = Util.index_of_n_forward(read_array, scan_index) + 1;
                if (read_size == SOFT_READ_BUF_SIZE) {
                    scan_end = Util.index_of_n_forward(read_array, READ_BUF_SIZE) + 1;
                } else {
                    scan_end = read_size;
                }
                for (; scan_index < scan_end; ) {
                    i = index_of_comma(read_array, scan_index);
                    value1 = LongUtil.read_long(read_array, scan_index, i);
                    scan_index = i + 1;
                    i = index_of_n(read_array, scan_index);
                    value2 = LongUtil.read_long(read_array, scan_index, i);
                    scan_index = i + 1;
                    unsafe.putLong(order_write_index, value1);
                    unsafe.putLong(part_write_index, value2);
                    order_write_index += 8;
                    part_write_index += 8;
                }
                MapFreeThread.free(read_task.read_buf);
                free_read_tasks.offer(read_task);
                parse_task.order_buf.write_index = order_write_index;
                parse_task.part_buf.write_index = part_write_index;
                read_task.table.work_parse_tasks.offer(parse_task);
            }
            flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void flush() {
        SimpleAnalyticDB.parse_waiter.countDown();
    }

    static int index(long value) {
        return (int) (value >>> Constants.BUCKET_COUNT_MOVE);
    }

}
