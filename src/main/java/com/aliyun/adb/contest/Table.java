package com.aliyun.adb.contest;

import com.aliyun.adb.contest.common.ByteBuf;
import com.aliyun.adb.contest.common.Constants;
import com.aliyun.adb.contest.task.ParseTask;
import com.aliyun.adb.contest.task.ReadThread;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author: wangkai
 * @create: 2021-06-03 22:21
 **/
public class Table extends Constants {

    static int block_read_task_size = 0;

    public static final String L_ORDER_KEY = "L_ORDERKEY";
    public static final String O_ORDER_KEY = "O_ORDERKEY";
    public static final String L_PART_KEY = "L_PARTKEY";
    public static final String O_CUST_KEY = "O_CUSTKEY";

    public int index;
    public long read_pos;
    public long file_size;
    public String work_dir;
    public String table_name;
    public Path table_file_path;
    public long order_key_size;
    public long part_key_size;

    public final long[] order_key_size_array = new long[BUCKET_COUNT];
    public final long[] part_key_size_array = new long[BUCKET_COUNT];
    public final Bucket[] order_key_bucket_array = new Bucket[BUCKET_COUNT];
    public final Bucket[] part_key_bucket_array = new Bucket[BUCKET_COUNT];

    public final Queue<ParseTask> work_parse_tasks = new ConcurrentLinkedQueue<>();

    public Table(String tpch_dir, String work_dir, String table_name, int index) {
        this.index = index;
        this.work_dir = work_dir;
        this.table_name = table_name;
        this.table_file_path = Paths.get(tpch_dir, table_name);
        this.file_size = table_file_path.toFile().length();
        this.init_read_pos();
    }

    private void init_read_pos() {
        long thread_size = READ_THREAD_SIZE / 2;
        long file_size = this.file_size;
        long read_pos = 0;
        long block_count = ((file_size - 1 + READ_BUF_SIZE) / READ_BUF_SIZE);
        block_count = (block_count - 1 + thread_size) / thread_size;
        long block_size = block_count * READ_BUF_SIZE;
        long redundant_block_size = block_size + READ_BUF_PADDING;
        for (; read_pos < file_size; ) {
            int thread_index = block_read_task_size++;
            ReadThread read_thread = ReadThread.READ_THREADS[thread_index];
            long read_size = Math.min(file_size - read_pos, redundant_block_size);
            read_thread.read_pos = read_pos;
            if (read_size == redundant_block_size) {
                read_pos += block_size;
            } else {
                read_pos += read_size;
            }
            read_thread.read_limit = read_pos;
        }
    }

    public void count_size_array(ByteBuf buf) throws IOException {
        Bucket[] order_key_bucket_array = this.order_key_bucket_array;
        Bucket[] part_key_bucket_array = this.part_key_bucket_array;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            order_key_bucket_array[i].file_size = (int) (order_key_size_array[i] * 7);
            part_key_bucket_array[i].file_size = (int) (part_key_size_array[i] * 7);
        }
        for (int i = 0; i < BUCKET_COUNT - 1; i++) {
            order_key_size_array[i + 1] += order_key_size_array[i];
            part_key_size_array[i + 1] += part_key_size_array[i];
        }
        for (int i = 0; i < BUCKET_COUNT; i++) {
            buf.write_long(order_key_size_array[i]);
            buf.write_long(part_key_size_array[i]);
        }
        order_key_size = order_key_size_array[BUCKET_COUNT - 1];
        part_key_size = part_key_size_array[BUCKET_COUNT - 1];
    }

    public void load_size_array(ByteBuf buf) {
        Bucket[] order_key_bucket_array = this.order_key_bucket_array;
        Bucket[] part_key_bucket_array = this.part_key_bucket_array;
        long[] order_key_size_array = this.order_key_size_array;
        long[] part_key_size_array = this.part_key_size_array;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            order_key_size_array[i] = buf.read_long();
            part_key_size_array[i] = buf.read_long();
        }
        order_key_bucket_array[0].file_size = ((int) order_key_size_array[0]) * 7;
        part_key_bucket_array[0].file_size = ((int) part_key_size_array[0]) * 7;
        for (int i = 1; i < BUCKET_COUNT; i++) {
            order_key_bucket_array[i].file_size = ((int) (order_key_size_array[i] - order_key_size_array[i - 1])) * 7;
            part_key_bucket_array[i].file_size = ((int) (part_key_size_array[i] - part_key_size_array[i - 1])) * 7;
        }
        order_key_size = order_key_size_array[BUCKET_COUNT - 1];
        part_key_size = part_key_size_array[BUCKET_COUNT - 1];
    }

    public int get_header_size() {
        return L_ORDER_KEY.length() + L_PART_KEY.length() + 2;
    }

    public void init_buckets() {
        int bucket_index = index * BUCKET_COUNT * 2;
        Bucket[] order_key_bucket_array = this.order_key_bucket_array;
        Bucket[] part_key_bucket_array = this.part_key_bucket_array;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            order_key_bucket_array[i] = new Bucket(bucket_index++, i, this);
            part_key_bucket_array[i] = new Bucket(bucket_index++, i, this);
        }
    }

    public void init_bucket_rw_channel() throws IOException {
        Bucket[] order_key_bucket_array = this.order_key_bucket_array;
        Bucket[] part_key_bucket_array = this.part_key_bucket_array;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            order_key_bucket_array[i].open_read_write();
            part_key_bucket_array[i].open_read_write();
        }
    }

    private long quantile(long index, long[] size_array, Bucket[] bucket_array) throws Exception {
        int local_index = -1;
        int bucket_index = -1;
        int search = (int) ((index - 1) / SORT_ARRAY_LEN);
        if (search < 0) {
            search = 0;
        } else if (search >= BUCKET_COUNT) {
            search = BUCKET_COUNT - 1;
        }
        if (size_array[search] > index) {
            for (int i = search - 1; i >= 0; i--) {
                if (size_array[i] <= index) {
                    bucket_index = i + 1;
                    local_index = (int) (index - size_array[i]);
                    break;
                }
            }
            if (bucket_index == -1) {
                bucket_index = 0;
                local_index = (int) index;
            }
        } else {
            for (int i = search + 1; i < BUCKET_COUNT; i++) {
                if (size_array[i] > index) {
                    bucket_index = i;
                    local_index = (int) (index - size_array[i - 1]);
                    break;
                }
            }
        }
        return bucket_array[bucket_index].find(local_index);
    }

    public String quantile(String column, double percentile) throws Exception {
        long value;
        if (L_ORDER_KEY.equals(column) || O_ORDER_KEY.equals(column)) {
            long rank = Math.round(order_key_size * percentile) - 1;
            value = quantile(rank, order_key_size_array, order_key_bucket_array);
        } else {
            long rank = Math.round(part_key_size * percentile) - 1;
            value = quantile(rank, part_key_size_array, part_key_bucket_array);
        }
        return String.valueOf(value);
    }

    public static long[] get_size_array(int index) {
        if (index == 0) {
            return SimpleAnalyticDB.line_item.order_key_size_array;
        } else if (index == 1) {
            return SimpleAnalyticDB.line_item.part_key_size_array;
        } else if (index == 2) {
            return SimpleAnalyticDB.orders.order_key_size_array;
        } else if (index == 3) {
            return SimpleAnalyticDB.orders.part_key_size_array;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static Bucket[] get_bucket_array(int index) {
        if (index == 0) {
            return SimpleAnalyticDB.line_item.order_key_bucket_array;
        } else if (index == 1) {
            return SimpleAnalyticDB.line_item.part_key_bucket_array;
        } else if (index == 2) {
            return SimpleAnalyticDB.orders.order_key_bucket_array;
        } else if (index == 3) {
            return SimpleAnalyticDB.orders.part_key_bucket_array;
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
