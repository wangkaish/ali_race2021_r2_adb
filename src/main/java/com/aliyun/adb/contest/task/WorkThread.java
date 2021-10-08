package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.Bucket;
import com.aliyun.adb.contest.SimpleAnalyticDB;
import com.aliyun.adb.contest.Table;
import com.aliyun.adb.contest.common.ByteBuf;
import com.aliyun.adb.contest.common.Constants;
import com.aliyun.adb.contest.common.Util;

import java.io.IOException;
import java.util.Queue;

import static com.aliyun.adb.contest.common.Constants.*;
import static com.aliyun.adb.contest.common.Util.unsafe;

/**
 * @author: wangkai
 * @create: 2021-06-05 22:34
 **/
public class WorkThread extends Thread {

    public static final WorkThread[] WORK_THREADS = new WorkThread[WORK_THREAD_SIZE];

    public final int index;
    public final Table table;
    public final long order_buf_temp = unsafe.allocateMemory(CP_LV_1_BUF_CAP);
    public final long part_buf_temp = unsafe.allocateMemory(CP_LV_1_BUF_CAP);
    public final int[] order_size_array = new int[BUCKET_COUNT];
    public final int[] part_size_array = new int[BUCKET_COUNT];
    public final int[] order_pos_array = new int[BUCKET_COUNT];
    public final int[] part_pos_array = new int[BUCKET_COUNT];
    public final ByteBuf[] order_byte_buf_array = new ByteBuf[BUCKET_COUNT];
    public final ByteBuf[] part_byte_buf_array = new ByteBuf[BUCKET_COUNT];

    public WorkThread(int index) {
        super("Work-Thread-" + index);
        this.index = index;
        this.table = SimpleAnalyticDB.get_table(index, WORK_THREAD_SIZE);
    }

    public static void start_threads() {
        init_write_tasks();
        for (int i = 0; i < WORK_THREAD_SIZE; i++) {
            WORK_THREADS[i] = new WorkThread(i);
            WORK_THREADS[i].start();
        }
    }

    private void count_bucket_size() {
        long[] order_key_size_array = table.order_key_size_array;
        long[] part_key_size_array = table.part_key_size_array;
        int[] order_size_array = this.order_size_array;
        int[] part_size_array = this.part_size_array;
        for (int j = 0; j < BUCKET_COUNT; j++) {
            order_key_size_array[j] += order_size_array[j];
            part_key_size_array[j] += part_size_array[j];
        }
    }

    public static void count_all_bucket_size() throws IOException {
        for (int i = 0; i < WORK_THREAD_SIZE; i++) {
            WORK_THREADS[i].count_bucket_size();
        }
    }

    private static void init_write_tasks() {
        for (int i = 0; i < WRITE_THREAD_SIZE; i++) {
            int block = Constants.WRITE_BUF_SIZE;
            int size = Constants.WRITE_TASK_COUNT * block;
            ByteBuf buf = ByteBuf.allocate(size);
            for (int j = 0; j < Constants.WRITE_TASK_COUNT; j++) {
                ByteBuf slice = buf.slice(j * block, block);
                WriteThread.free_write_tasks.offer(new WriteTask(slice));
            }
        }
    }

    public static void finish() {
        SimpleAnalyticDB.line_item.work_parse_tasks.offer(new ParseTask(true));
        SimpleAnalyticDB.orders.work_parse_tasks.offer(new ParseTask(true));
    }

    @Override
    public void run() {
        try {
            ByteBuf[] order_byte_buf_array = this.order_byte_buf_array;
            ByteBuf[] part_byte_buf_array = this.part_byte_buf_array;
            Bucket[] order_key_bucket_array = table.order_key_bucket_array;
            Bucket[] part_key_bucket_array = table.part_key_bucket_array;

            Queue<ParseTask> work_parse_tasks = table.work_parse_tasks;
            Queue<ParseTask> free_parse_tasks = ParseThread.free_parse_tasks;
            Queue<WriteTask> free_write_tasks = WriteThread.free_write_tasks;

            long order_buf_temp = this.order_buf_temp;
            long part_buf_temp = this.part_buf_temp;
            int[] order_size_array = this.order_size_array;
            int[] part_size_array = this.part_size_array;
            int[] order_pos_array = this.order_pos_array;
            int[] part_pos_array = this.part_pos_array;
            for (int i = 0; i < BUCKET_COUNT; i++) {
                order_pos_array[i] = i << CP_LV_1_COUNT_MOVE;
                part_pos_array[i] = i << CP_LV_1_COUNT_MOVE;
            }

            for (int i = 0; i < BUCKET_COUNT; i++) {
                order_byte_buf_array[i] = Util.take_write_task(free_write_tasks).buf;
                order_byte_buf_array[i].task.bucket = order_key_bucket_array[i];
                part_byte_buf_array[i] = Util.take_write_task(free_write_tasks).buf;
                part_byte_buf_array[i].task.bucket = part_key_bucket_array[i];
            }

            ParseTask parse_task;
            for (; ; ) {
                parse_task = work_parse_tasks.poll();
                if (parse_task == null) {
                    WriteTask write_task = WriteThread.work_write_tasks.poll();
                    if (write_task == null) {
                        Thread.sleep(1);
                    } else {
                        write_task.write();
                        WriteThread.free_write_tasks.offer(write_task);
                    }
                    continue;
                }
                if (parse_task.is_final_block) {
                    break;
                }
                ByteBuf order_buf = parse_task.order_buf;
                ByteBuf part_buf = parse_task.part_buf;

                store_order_values(order_buf, order_buf_temp, order_pos_array);
                store_part_values(part_buf, part_buf_temp, part_pos_array);
                free_parse_tasks.offer(parse_task);
            }
            flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void store_order_values(
            ByteBuf long_buf, long lv_1_buf, int[] pos_array) throws InterruptedException {
        long buf_offset = long_buf.offset;
        long buf_write_index = long_buf.write_index;
        int size = (int) (buf_write_index - buf_offset) >>> 3;
        long value;
        for (int i = 0; i < size; i++) {
            value = unsafe.getLong(buf_offset + ((long) i << 3));
            int index = index(value);
            int put_pos = pos_array[index]++;
            if (((put_pos) & CP_LV_1_COUNT_MASK) == CP_LV_1_COUNT_MASK) {
                long put_address = lv_1_buf + put_pos * 7L;
                for (int j = 0; j < 7; j++) {
                    unsafe.putByte(put_address++, (byte) value);
                    value >>= 8;
                }
                do_flush(lv_1_buf, pos_array, order_size_array, order_byte_buf_array, table.order_key_bucket_array);
            } else {
                unsafe.putLong(lv_1_buf + put_pos * 7L, value);
            }
        }
    }

    private void store_part_values(
            ByteBuf long_buf, long lv_1_buf, int[] pos_array) throws InterruptedException {
        long buf_offset = long_buf.offset;
        long buf_write_index = long_buf.write_index;
        int size = (int) (buf_write_index - buf_offset) >>> 3;
        long value;
        for (int i = 0; i < size; i++) {
            value = unsafe.getLong(buf_offset + ((long) i << 3));
            int index = index(value);
            int put_pos = pos_array[index]++;
            if (((put_pos) & CP_LV_1_COUNT_MASK) == CP_LV_1_COUNT_MASK) {
                long put_address = lv_1_buf + put_pos * 7L;
                for (int j = 0; j < 7; j++) {
                    unsafe.putByte(put_address++, (byte) value);
                    value >>= 8;
                }
                do_flush(lv_1_buf, pos_array, part_size_array, part_byte_buf_array, table.part_key_bucket_array);
            } else {
                unsafe.putLong(lv_1_buf + put_pos * 7L, value);
            }
        }
    }

    private void do_flush(
            long buf_temp, int[] pos_array, int[] size_array, ByteBuf[] buf_array, Bucket[] bucket_array) throws InterruptedException {
        Queue<WriteTask> work_write_tasks = WriteThread.work_write_tasks;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            int pos_base = i << CP_LV_1_COUNT_MOVE;
            long size = pos_array[i] - pos_base;
            long remain = size * 7;
            pos_array[i] = pos_base;
            size_array[i] += size;
            ByteBuf buf = buf_array[i];
            long write_index = buf.write_index;
            if (write_index + remain > buf.limit) {
                WriteThread.work_write_tasks.offer(buf.task);
                buf = buf_array[i] = Util.take_write_task(WriteThread.free_write_tasks).buf.clear();
                buf.task.bucket = bucket_array[i];
                write_index = buf.write_index;
            }
            unsafe.copyMemory(buf_temp + (pos_base * 7L), write_index, remain);
            buf.write_index = (write_index + remain);
        }
    }

    private static void do_flush_final(
            long buf_temp, int[] pos_array, int[] size_array, ByteBuf[] buf_array, Bucket[] bucket_array) {
        Queue<WriteTask> work_write_tasks = WriteThread.work_write_tasks;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            long pos_base = (((long) i) << CP_LV_1_COUNT_MOVE);
            long order_size = pos_array[i] - pos_base;
            long order_remain = order_size * 7;
            size_array[i] += order_size;
            ByteBuf buf = buf_array[i];
            long order_write_index = buf.write_index;
            unsafe.copyMemory(buf_temp + (pos_base * 7), order_write_index, order_remain);
            buf.write_index = (order_write_index + order_remain);
        }
    }

    private void flush() throws IOException {
        do_flush_final(order_buf_temp, order_pos_array, order_size_array, order_byte_buf_array, table.order_key_bucket_array);
        do_flush_final(part_buf_temp, part_pos_array, part_size_array, part_byte_buf_array, table.part_key_bucket_array);
        Queue<WriteTask> work_write_tasks = WriteThread.work_write_tasks;
        for (int i = 0; i < BUCKET_COUNT / 2; i++) {
            work_write_tasks.offer(order_byte_buf_array[i].task);
            work_write_tasks.offer(part_byte_buf_array[i].task);
        }
        for (int i = BUCKET_COUNT / 2; i < BUCKET_COUNT; i++) {
            order_byte_buf_array[i].task.write();
            part_byte_buf_array[i].task.write();
        }
        SimpleAnalyticDB.work_waiter.countDown();
    }

    static int index(long value) {
        return (int) (value >>> Constants.BUCKET_COUNT_MOVE);
    }

}
