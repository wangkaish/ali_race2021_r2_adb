package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.Query;
import com.aliyun.adb.contest.common.ByteBuf;
import com.aliyun.adb.contest.common.Constants;
import com.aliyun.adb.contest.common.SortArray;

import java.nio.channels.FileChannel;

/**
 * @author: wangkai
 * @create: 2021-07-19 21:48
 **/
public class SortTask extends Constants {

    public FileChannel channel;
    public long read_pos;
    public int read_size;
    public final Query query;
    public final SortArray sort_array = new SortArray();
    public final ByteBuf read_buf = ByteBuf.allocate(QUERY_READ_BUF_SIZE / SORT_THREAD_SIZE);

    public SortTask(Query query) {
        this.query = query;
    }
}
