package com.aliyun.adb.contest.common;

import static com.aliyun.adb.contest.common.Util.unsafe;

/**
 * @author: wangkai
 * @create: 2021-08-14 16:53
 **/
public class LongUtil {

    public static long read_long(long array, int from, int to) {
        long __from = array + from;
        int size = to - from;
        if (size == 19) {
            return read_long_19(__from);
        } else if (size == 18) {
            return read_long_18(__from);
        } else if (size == 17) {
            return read_long_17(__from);
        } else if (size == 16) {
            return read_long_16(__from);
        } else {
            long value = 0;
            for (int i = 0; i < size; i++) {
                value = value * 10 + (unsafe.getByte(__from + i) - '0');
            }
            return value;
        }
    }

    private static long read_long_16(long array) {
        long v0 = (unsafe.getByte(array + 0) - '0') * 1000000000000000L;
        long v1 = (unsafe.getByte(array + 1) - '0') * 100000000000000L;
        long v2 = (unsafe.getByte(array + 2) - '0') * 10000000000000L;
        long v3 = (unsafe.getByte(array + 3) - '0') * 1000000000000L;
        long v4 = (unsafe.getByte(array + 4) - '0') * 100000000000L;
        long v5 = (unsafe.getByte(array + 5) - '0') * 10000000000L;
        long v6 = (unsafe.getByte(array + 6) - '0') * 1000000000L;
        long v7 = (unsafe.getByte(array + 7) - '0') * 100000000L;
        long v8 = (unsafe.getByte(array + 8) - '0') * 10000000L;
        long v9 = (unsafe.getByte(array + 9) - '0') * 1000000L;
        long v10 = (unsafe.getByte(array + 10) - '0') * 100000L;
        long v11 = (unsafe.getByte(array + 11) - '0') * 10000L;
        long v12 = (unsafe.getByte(array + 12) - '0') * 1000L;
        long v13 = (unsafe.getByte(array + 13) - '0') * 100L;
        long v14 = (unsafe.getByte(array + 14) - '0') * 10L;
        long v15 = (unsafe.getByte(array + 15) - '0') * 1L;
        return v0 + v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14 + v15;
    }

    private static long read_long_17(long array) {
        long v0 = (unsafe.getByte(array + 0) - '0') * 10000000000000000L;
        long v1 = (unsafe.getByte(array + 1) - '0') * 1000000000000000L;
        long v2 = (unsafe.getByte(array + 2) - '0') * 100000000000000L;
        long v3 = (unsafe.getByte(array + 3) - '0') * 10000000000000L;
        long v4 = (unsafe.getByte(array + 4) - '0') * 1000000000000L;
        long v5 = (unsafe.getByte(array + 5) - '0') * 100000000000L;
        long v6 = (unsafe.getByte(array + 6) - '0') * 10000000000L;
        long v7 = (unsafe.getByte(array + 7) - '0') * 1000000000L;
        long v8 = (unsafe.getByte(array + 8) - '0') * 100000000L;
        long v9 = (unsafe.getByte(array + 9) - '0') * 10000000L;
        long v10 = (unsafe.getByte(array + 10) - '0') * 1000000L;
        long v11 = (unsafe.getByte(array + 11) - '0') * 100000L;
        long v12 = (unsafe.getByte(array + 12) - '0') * 10000L;
        long v13 = (unsafe.getByte(array + 13) - '0') * 1000L;
        long v14 = (unsafe.getByte(array + 14) - '0') * 100L;
        long v15 = (unsafe.getByte(array + 15) - '0') * 10L;
        long v16 = (unsafe.getByte(array + 16) - '0') * 1L;
        return v0 + v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14 + v15 + v16;
    }

    private static long read_long_18(long array) {
        long v0 = (unsafe.getByte(array + 0) - '0') * 100000000000000000L;
        long v1 = (unsafe.getByte(array + 1) - '0') * 10000000000000000L;
        long v2 = (unsafe.getByte(array + 2) - '0') * 1000000000000000L;
        long v3 = (unsafe.getByte(array + 3) - '0') * 100000000000000L;
        long v4 = (unsafe.getByte(array + 4) - '0') * 10000000000000L;
        long v5 = (unsafe.getByte(array + 5) - '0') * 1000000000000L;
        long v6 = (unsafe.getByte(array + 6) - '0') * 100000000000L;
        long v7 = (unsafe.getByte(array + 7) - '0') * 10000000000L;
        long v8 = (unsafe.getByte(array + 8) - '0') * 1000000000L;
        long v9 = (unsafe.getByte(array + 9) - '0') * 100000000L;
        long v10 = (unsafe.getByte(array + 10) - '0') * 10000000L;
        long v11 = (unsafe.getByte(array + 11) - '0') * 1000000L;
        long v12 = (unsafe.getByte(array + 12) - '0') * 100000L;
        long v13 = (unsafe.getByte(array + 13) - '0') * 10000L;
        long v14 = (unsafe.getByte(array + 14) - '0') * 1000L;
        long v15 = (unsafe.getByte(array + 15) - '0') * 100L;
        long v16 = (unsafe.getByte(array + 16) - '0') * 10L;
        long v17 = (unsafe.getByte(array + 17) - '0') * 1L;
        return v0 + v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14 + v15 + v16 + v17;
    }

    private static long read_long_19(long array) {
        long v0 = (unsafe.getByte(array + 0) - '0') * 1000000000000000000L;
        long v1 = (unsafe.getByte(array + 1) - '0') * 100000000000000000L;
        long v2 = (unsafe.getByte(array + 2) - '0') * 10000000000000000L;
        long v3 = (unsafe.getByte(array + 3) - '0') * 1000000000000000L;
        long v4 = (unsafe.getByte(array + 4) - '0') * 100000000000000L;
        long v5 = (unsafe.getByte(array + 5) - '0') * 10000000000000L;
        long v6 = (unsafe.getByte(array + 6) - '0') * 1000000000000L;
        long v7 = (unsafe.getByte(array + 7) - '0') * 100000000000L;
        long v8 = (unsafe.getByte(array + 8) - '0') * 10000000000L;
        long v9 = (unsafe.getByte(array + 9) - '0') * 1000000000L;
        long v10 = (unsafe.getByte(array + 10) - '0') * 100000000L;
        long v11 = (unsafe.getByte(array + 11) - '0') * 10000000L;
        long v12 = (unsafe.getByte(array + 12) - '0') * 1000000L;
        long v13 = (unsafe.getByte(array + 13) - '0') * 100000L;
        long v14 = (unsafe.getByte(array + 14) - '0') * 10000L;
        long v15 = (unsafe.getByte(array + 15) - '0') * 1000L;
        long v16 = (unsafe.getByte(array + 16) - '0') * 100L;
        long v17 = (unsafe.getByte(array + 17) - '0') * 10L;
        long v18 = (unsafe.getByte(array + 18) - '0') * 1L;
        return v0 + v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14 + v15 + v16 + v17 + v18;
    }


}
