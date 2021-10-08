package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.Bucket;
import com.aliyun.adb.contest.common.ByteBuf;
import com.aliyun.adb.contest.common.Util;

import java.io.IOException;

/**
 * @author: wangkai
 * @create: 2021-06-30 22:31
 **/
public class WriteTask {

    public Bucket bucket;
    public final ByteBuf buf;
    public final boolean is_finally_block;

    public WriteTask() {
        this.buf = null;
        this.is_finally_block = true;
    }

    public WriteTask(ByteBuf buf) {
        this.buf = buf;
        this.buf.task = this;
        this.is_finally_block = false;
    }

    public void write() throws IOException {
        Util.complete_write(bucket.channel, buf.nioWriteBuffer());
    }

}
