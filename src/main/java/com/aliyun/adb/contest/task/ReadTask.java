package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.Table;
import com.aliyun.adb.contest.common.Constants;

import java.nio.ByteBuffer;

/**
 * @author: wangkai
 * @create: 2021-06-26 16:42
 **/
public class ReadTask extends Constants {

    public final boolean is_final_block;
    public ByteBuffer read_buf;
    public long read_pos;
    public int read_size;
    public Table table;

    public ReadTask(boolean is_final_block) {
        this.is_final_block = is_final_block;
    }

}
