package com.aliyun.adb.contest.common;

import java.lang.reflect.Field;

/**
 * @author: wangkai
 * @create: 2021-06-03 22:17
 **/
public class Constants {

    public static final boolean QUICK_SELECT = true;
    public static final boolean CHECK_RANGE = false;
    public static final int SIZE_1_M = 1024 * 1024;
    public static final int MAX_LINE_COUNT = 10_0000_0000;
    public static final int BUCKET_COUNT = 2048 * 1;
    public static final int BUCKET_COUNT_MOVE = 63 - Util.log2(BUCKET_COUNT);

    public static final int READ_THREAD_SIZE = 6;
    public static final int PARSE_THREAD_SIZE = 3;
    public static final int WORK_THREAD_SIZE = 2;
    public static final int WRITE_THREAD_SIZE = 5;
    public static final int SORT_THREAD_SIZE = 3;

    public static final int SORT_BUCKET_COUNT = 256;
    public static final int SORT_ARRAY_LEN = MAX_LINE_COUNT / BUCKET_COUNT;
    public static final int SORT_ARRAY_FIXED_PART = SORT_ARRAY_LEN / SORT_BUCKET_COUNT;
    public static final int SORT_ARRAY_PART = SORT_ARRAY_FIXED_PART * 4 / 3;
    public static final int SORT_MOVE_STEP = 63 - Util.log2(SORT_BUCKET_COUNT * BUCKET_COUNT);
    public static final int SORT_MASK = SORT_BUCKET_COUNT - 1;
    public static final int SORT_ARRAY_THREAD_PART = SORT_ARRAY_PART / SORT_THREAD_SIZE;

    public static final int CP_LV_1_COUNT = 64;
    public static final int CP_LV_1_COUNT_MASK = CP_LV_1_COUNT - 1;
    public static final int CP_LV_1_COUNT_MOVE = Util.log2(CP_LV_1_COUNT);
    public static final int CP_LV_1_BUF = CP_LV_1_COUNT * 7;
    public static final int CP_LV_1_BUF_CAP = CP_LV_1_BUF * BUCKET_COUNT;

    public static final int WRITE_TASK_COUNT = BUCKET_COUNT * 2;
    public static final int WRITE_BUF_SIZE = 7 * 1024 * 18;
    public static final long ALL_WRITE_BUF_SIZE = 1L * WRITE_TASK_COUNT * WRITE_THREAD_SIZE * WRITE_BUF_SIZE / SIZE_1_M;

    public static final int READ_TASK_COUNT = 8;
    public static final int READ_BUF_MB = 16;
    public static final int READ_BUF_PADDING = 64;
    public static final int READ_BUF_SIZE = 1024 * 1024 * READ_BUF_MB;
    public static final int SOFT_READ_BUF_SIZE = READ_BUF_SIZE + READ_BUF_PADDING;
    public static final int ALL_READ_BUF_SIZE = READ_BUF_SIZE * READ_TASK_COUNT * READ_THREAD_SIZE / SIZE_1_M;

    public static final int QUERY_READ_BUF_SIZE = 1024 * 1024 * (4096 / BUCKET_COUNT) * 2;
    public static final int QUERY_RET_MOVE = BUCKET_COUNT == 2048 ? 4 : 5;

    public static final int PARSE_COUNT = 1024 * 64 * READ_BUF_MB / 2;

    public static final int NANO = 1000000;

    public static void print() {
        try {
            Field[] fields = Constants.class.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);
                Object o = field.get(null);
                Util.debug(field.getName() + ": " + o);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

}
