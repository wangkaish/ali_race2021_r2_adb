package com.aliyun.adb.contest;

import com.aliyun.adb.contest.common.ByteBuf;
import com.aliyun.adb.contest.common.Constants;
import com.aliyun.adb.contest.common.Util;
import com.aliyun.adb.contest.spi.AnalyticDB;
import com.aliyun.adb.contest.task.*;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SimpleAnalyticDB extends Constants implements AnalyticDB {

    public static final long start_time = System.currentTimeMillis();
    public static long step_1_start_time;

    public static final String META_DATA = "meta_data";
    public static final String LINE_ITEM = "lineitem";
    public static final String ORDERS = "orders";
    public static final int META_FILE_SIZE = BUCKET_COUNT * 4 * 8 + 8;
    public static final CountDownLatch read_waiter = new CountDownLatch(READ_THREAD_SIZE);
    public static final CountDownLatch parse_waiter = new CountDownLatch(PARSE_THREAD_SIZE);
    public static final CountDownLatch work_waiter = new CountDownLatch(WORK_THREAD_SIZE);
    public static final CountDownLatch write_waiter = new CountDownLatch(WRITE_THREAD_SIZE);

    public static Table line_item;
    public static Table orders;

    public static Table get_table(int index, int all) {
        return index < (all / 2) ? line_item : orders;
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        ReadThread.init_threads();

        line_item = new Table(tpchDataFileDir, workspaceDir, LINE_ITEM, 0);
        orders = new Table(tpchDataFileDir, workspaceDir, ORDERS, 1);

        Path meta_path = Paths.get(workspaceDir, META_DATA);
        if (meta_path.toFile().exists()) {
            line_item.init_buckets();
            orders.init_buckets();
            ByteBuf meta_buf = ByteBuf.allocate(META_FILE_SIZE);
            FileChannel channel = Util.open_read_only(meta_path);
            Util.complete_read(channel, meta_buf, META_FILE_SIZE);
            line_item.load_size_array(meta_buf);
            orders.load_size_array(meta_buf);
            step_1_start_time = meta_buf.get_long(META_FILE_SIZE - 8);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                long now = System.currentTimeMillis();
                long cost = now - start_time;
                long all_cost = now - step_1_start_time;
                System.out.println(cost);
                System.out.println(all_cost);
            }));
            return;
        }

        ReadThread.start_threads();


        line_item.init_buckets();
        orders.init_buckets();

        ParseThread.start_threads();

        MapFreeThread.start();

        WorkThread.start_threads();

        line_item.init_bucket_rw_channel();
        orders.init_bucket_rw_channel();

        WriteThread.start_threads();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            long now = System.currentTimeMillis();
            long cost = now - start_time;
            System.out.println(cost);
        }));

        read_waiter.await(60, TimeUnit.SECONDS);

        ParseThread.finish();
        parse_waiter.await(6, TimeUnit.SECONDS);

        WorkThread.finish();
        work_waiter.await(6, TimeUnit.SECONDS);

        WriteThread.finish();
        WorkThread.count_all_bucket_size();
        write_waiter.await(6, TimeUnit.SECONDS);

        ByteBuf meta_buf = ByteBuf.allocate(META_FILE_SIZE);
        line_item.count_size_array(meta_buf);
        orders.count_size_array(meta_buf);
        meta_buf.write_long(start_time);
        FileChannel channel = Util.open_read_write(meta_path);
        Util.complete_write(channel, meta_buf.nioWriteBuffer());
    }

    @Override
    public String quantile(String tableName, String columnName, double percentile) throws Exception {
        if (LINE_ITEM.equals(tableName)) {
            return line_item.quantile(columnName, percentile);
        } else {
            return orders.quantile(columnName, percentile);
        }
    }

}
