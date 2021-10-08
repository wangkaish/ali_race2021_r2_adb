package com.aliyun.adb.contest;

import com.aliyun.adb.contest.common.Constants;
import com.aliyun.adb.contest.common.SortArray;
import com.aliyun.adb.contest.common.Util;
import com.aliyun.adb.contest.task.SortTask;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author: wangkai
 * @create: 2021-06-29 22:20
 **/
public class Bucket extends Constants {

    static final ThreadLocal<Query> QUERY = ThreadLocal.withInitial(Query::new);

    public int bucket_index;
    public int local_index;
    public int file_size;
    public Table table;
    public FileChannel channel;
    public String file_name;
    public Path path;

    public Bucket(int bucket_index, int local_index, Table table) {
        this.bucket_index = bucket_index;
        this.local_index = local_index;
        this.table = table;
        this.file_name = String.valueOf(bucket_index);
    }

    public FileChannel open_read_only() throws IOException {
        Path path = Paths.get(table.work_dir, file_name);
        return Util.open_read_only(path);
    }

    public void open_read_write() throws IOException {
        Path path = Paths.get(table.work_dir, file_name);
        this.channel = Util.open_read_write(path);
    }

    private void init_read_channel(Query query) throws IOException {
        FileChannel[] channels = query.channels_1;
        if (channels[bucket_index] == null) {
            channels[bucket_index] = open_read_only();
            query.channels_2[bucket_index] = open_read_only();
            if (SORT_THREAD_SIZE == 3) {
                query.channels_3[bucket_index] = open_read_only();
            }
        }
    }

    public long find(int index) throws Exception {
        Query query = QUERY.get();
        long value = find_switch(query, index);
        return value | ((long) (local_index >>> QUERY_RET_MOVE) << 56);
    }

    private long find_switch(Query query, int index) throws Exception {
        if (SORT_THREAD_SIZE == 3) {
            return find_3(query, index);
        } else {
            return find_2(query, index);
        }
    }

    private long find_2(Query query, int index) throws Exception {
        int read_pos = 0;
        int file_size = this.file_size;
        int read_size = ((file_size / 7) / SORT_THREAD_SIZE) * 7;
        int back_read_size = file_size - read_size;
        init_read_channel(query);

        query.begin_task();

        SortTask sort_task_2 = query.sort_task_2;
        sort_task_2.read_pos = read_pos;
        sort_task_2.read_size = read_size;
        sort_task_2.channel = query.channels_2[bucket_index];
        query.sort_thread_2.work_tasks.put(sort_task_2);
        read_pos += read_size;

        FileChannel channel = query.channels_1[bucket_index];
        SortArray sort_array = query.sort_array_1;
        sort_array.write(channel, read_pos, file_size - read_pos, query.read_buf);

        query.wait_task_finish();

        return sort_array.get_long(index, sort_task_2.sort_array);
    }

    private long find_3(Query query, int index) throws Exception {
        int read_pos = 0;
        int file_size = this.file_size;
        int read_size = ((file_size / 7) / SORT_THREAD_SIZE) * 7;
        int back_read_size = file_size - read_size;
        init_read_channel(query);

        query.begin_task();

        SortTask sort_task_2 = query.sort_task_2;
        sort_task_2.read_pos = read_pos;
        sort_task_2.read_size = read_size;
        sort_task_2.channel = query.channels_2[bucket_index];
        query.sort_thread_2.work_tasks.put(sort_task_2);
        read_pos += read_size;

        SortTask sort_task_3 = query.sort_task_3;
        sort_task_3.read_pos = read_pos;
        sort_task_3.read_size = read_size;
        sort_task_3.channel = query.channels_3[bucket_index];
        query.sort_thread_3.work_tasks.put(sort_task_3);
        read_pos += read_size;

        FileChannel channel = query.channels_1[bucket_index];
        SortArray sort_array = query.sort_array_1;
        sort_array.write(channel, read_pos, file_size - read_pos, query.read_buf);

        query.wait_task_finish();
        query.wait_task_finish();

        return sort_array.get_long(index, sort_task_2.sort_array, sort_task_3.sort_array);
    }

}
