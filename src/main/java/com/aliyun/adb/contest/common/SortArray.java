package com.aliyun.adb.contest.common;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * @author: wangkai
 * @create: 2021-06-10 21:47
 **/
public class SortArray extends Constants {

    static final boolean PREFER_DIRECT = false;

    public final int[] pos_array = new int[SORT_BUCKET_COUNT];
    public final long[] local_array = new long[SORT_ARRAY_PART];
    public final long[] heap_array;
    public final ByteBuf direct_array;

    public SortArray() {
        if (PREFER_DIRECT) {
            direct_array = ByteBuf.allocate(SORT_ARRAY_THREAD_PART * SORT_BUCKET_COUNT * 8);
            heap_array = null;
        } else {
            heap_array = new long[SORT_ARRAY_THREAD_PART * SORT_BUCKET_COUNT];
            direct_array = null;
        }
    }

    public void write(FileChannel channel, long read_pos, int read_size, ByteBuf read_buf) throws IOException {
        int[] pos_array = this.pos_array;
        for (int i = 0; i < SORT_BUCKET_COUNT; i++) {
            pos_array[i] = i * SORT_ARRAY_THREAD_PART;
        }
        channel.position(read_pos);
        Util.complete_read(channel, read_buf, read_size);
        int size = read_size / 7;
        if (PREFER_DIRECT) {
            long address = direct_array.offset;
            for (int i = 0; i < size; i++) {
                long value = (read_buf.get_long(i * 7) & 0xffffff_ffffffffL);
                int index = (int) ((value >>> SORT_MOVE_STEP) & SORT_MASK);
                int pos = pos_array[index];
                Util.unsafe.putLong(address + ((long) pos << 3), value);
                pos_array[index] = pos + 1;
            }
        } else {
            long[] heap_array = this.heap_array;
            for (int i = 0; i < size; i++) {
                long value = (read_buf.get_long(i * 7) & 0xffffff_ffffffffL);
                int index = (int) ((value >>> SORT_MOVE_STEP) & SORT_MASK);
                int pos = pos_array[index];
                heap_array[pos] = value;
                pos_array[index] = pos + 1;
            }
        }
    }

    static void copy_long_array(long[] src, int src_offset, long[] dst, int dst_offset, int len) {
        for (int i = 0; i < len; i++) {
            dst[dst_offset + i] = src[src_offset + i];
        }
    }

    public long get_long(int index, SortArray sort_array_2) {
        int size = 0;
        int[] pos_array_1 = this.pos_array;
        int[] pos_array_2 = sort_array_2.pos_array;
        for (int i = 0; i < SORT_BUCKET_COUNT; i++) {
            int off = i * SORT_ARRAY_THREAD_PART;
            int to_1 = pos_array_1[i];
            int to_2 = pos_array_2[i];

            int local_size_1 = to_1 - off;
            int local_size_2 = to_2 - off;

            int local_size = local_size_1 + local_size_2;
            size += local_size;
            if (CHECK_RANGE) {
                if (local_size_1 > SORT_ARRAY_THREAD_PART
                        || local_size_2 > SORT_ARRAY_THREAD_PART) {
                    throw new RuntimeException("Sort overflow");
                }
            }
            if (size > index) {
                int local_array_pos = 0;
                if (PREFER_DIRECT) {
                    direct_array.get_long(local_array, local_array_pos, off, to_1);
                    local_array_pos += to_1 - off;
                    sort_array_2.direct_array.get_long(local_array, local_array_pos, off, to_2);
                } else {
                    copy_long_array(heap_array, off, local_array, local_array_pos, to_1 - off);
                    local_array_pos += to_1 - off;
                    copy_long_array(sort_array_2.heap_array, off, local_array, local_array_pos, to_2 - off);
                }

                int local_index = index;
                if (i > 0) {
                    local_index = index - (size - (local_size));
                }
                if (QUICK_SELECT) {
                    return QuickSelect.quick_select(local_array, 0, (local_size) - 1, local_index);
                } else {
                    Arrays.sort(local_array, 0, (local_size));
                    return local_array[local_index];
                }
            }
        }
        throw new RuntimeException("get_long_failed");
    }

    public long get_long(int index, SortArray sort_array_2, SortArray sort_array_3) {
        int size = 0;
        int[] pos_array_1 = this.pos_array;
        int[] pos_array_2 = sort_array_2.pos_array;
        int[] pos_array_3 = sort_array_3.pos_array;
        for (int i = 0; i < SORT_BUCKET_COUNT; i++) {
            int off = i * SORT_ARRAY_THREAD_PART;
            int to_1 = pos_array_1[i];
            int to_2 = pos_array_2[i];
            int to_3 = pos_array_3[i];

            int local_size_1 = to_1 - off;
            int local_size_2 = to_2 - off;
            int local_size_3 = to_3 - off;

            int local_size = local_size_1 + local_size_2 + local_size_3;
            size += local_size;
            if (CHECK_RANGE) {
                if (local_size_1 > SORT_ARRAY_THREAD_PART
                        || local_size_2 > SORT_ARRAY_THREAD_PART
                        || local_size_3 > SORT_ARRAY_THREAD_PART) {
                    throw new RuntimeException("Sort overflow");
                }
            }
            if (size > index) {
                int local_array_pos = 0;
                if (PREFER_DIRECT) {
                    direct_array.get_long(local_array, local_array_pos, off, to_1);
                    local_array_pos += to_1 - off;
                    sort_array_2.direct_array.get_long(local_array, local_array_pos, off, to_2);
                    local_array_pos += to_2 - off;
                    sort_array_3.direct_array.get_long(local_array, local_array_pos, off, to_3);
                } else {
                    copy_long_array(heap_array, off, local_array, local_array_pos, to_1 - off);
                    local_array_pos += to_1 - off;
                    copy_long_array(sort_array_2.heap_array, off, local_array, local_array_pos, to_2 - off);
                    local_array_pos += to_2 - off;
                    copy_long_array(sort_array_3.heap_array, off, local_array, local_array_pos, to_3 - off);
                }

                int local_index = index;
                if (i > 0) {
                    local_index = index - (size - (local_size));
                }
                if (QUICK_SELECT) {
                    return QuickSelect.quick_select(local_array, 0, (local_size) - 1, local_index);
                } else {
                    Arrays.sort(local_array, 0, (local_size));
                    return local_array[local_index];
                }
            }
        }
        throw new RuntimeException("get_long_failed");
    }

}

