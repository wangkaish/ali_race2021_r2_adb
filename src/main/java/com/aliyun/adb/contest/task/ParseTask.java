package com.aliyun.adb.contest.task;

import com.aliyun.adb.contest.common.ByteBuf;
import com.aliyun.adb.contest.common.Constants;

/**
 * @author: wangkai
 * @create: 2021-08-14 11:52
 **/
public class ParseTask {

    public final boolean is_final_block;
    public final ByteBuf order_buf;
    public final ByteBuf part_buf;

    public ParseTask(boolean is_final_block) {
        this.is_final_block = is_final_block;
        if (is_final_block) {
            order_buf = null;
            part_buf = null;
        } else {
            order_buf = ByteBuf.allocate(Constants.PARSE_COUNT << 3);
            part_buf = ByteBuf.allocate(Constants.PARSE_COUNT << 3);
        }
    }
}
