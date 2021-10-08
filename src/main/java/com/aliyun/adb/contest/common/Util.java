package com.aliyun.adb.contest.common;

import com.aliyun.adb.contest.task.ParseTask;
import com.aliyun.adb.contest.task.ReadTask;
import com.aliyun.adb.contest.task.WriteTask;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Queue;

/**
 * @author: wangkai
 * @create: 2021-05-29 10:24
 **/
public class Util {

    public static OpenOption[] READ_WRITE = new OpenOption[]{
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE
    };

    public static final OpenOption[] READ_ONLY = new OpenOption[]{StandardOpenOption.READ};
    public static final Unsafe unsafe = getUnsafe();
    public static final Field ADDRESS = get_address_field();

    private static Field get_address_field() {
        try {
            Field field = Buffer.class.getDeclaredField("address");
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static long get_address(ByteBuffer buffer) {
        try {
            return (Long) ADDRESS.get(buffer);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void free(ByteBuffer buffer) {
        ((DirectBuffer) buffer).cleaner().clean();
    }

    public static void debug(String msg) {
        System.out.println(appendTime(msg));
    }

    public static long past(long start_time) {
        return System.nanoTime() - start_time;
    }

    public static long now() {
        return System.nanoTime();
    }

    public static String appendTime(String msg) {
        String name = Thread.currentThread().getName();
        LocalDateTime localDateTime = LocalDateTime.now();
        String time = localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        return time + "[" + name + "] : " + msg;
    }

    public static long benchmark(String name, Runnable runnable) {
        return benchmark(name, true, runnable);
    }

    public static long benchmark(String name, boolean print_start, Runnable runnable) {
        if (print_start) {
            debug("--------------- Start to benchmark: " + name);
        }
        long startTime = System.currentTimeMillis();
        runnable.run();
        long cost = System.currentTimeMillis() - startTime;
        debug("Test [" + name + "] cost: " + cost);
        return cost;
    }

    public static void complete_write(FileChannel channel, ByteBuffer data) throws IOException {
        if (Constants.CHECK_RANGE) {
            channel.write(data);
            if (data.hasRemaining()) {
                debug("write_not_complete" + data.limit());
                for (; data.hasRemaining(); ) {
                    channel.write(data);
                }
            }
        } else {
            channel.write(data);
        }
    }

    public static void complete_read(RandomAccessFile channel, byte[] array, int buf_size) throws IOException {
        channel.readFully(array, 0, buf_size);
    }

    public static void complete_read(FileChannel channel, ByteBuffer buf, int buf_size) throws IOException {
        buf.clear().limit(buf_size);
        if (Constants.CHECK_RANGE) {
            channel.read(buf);
            if (buf.hasRemaining()) {
                for (; buf.hasRemaining(); ) {
                    int read = channel.read(buf);
                    if (read == -1) {
                        break;
                    }
                }
            }
        } else {
            channel.read(buf);
        }
        buf.flip();
    }

    public static void complete_read(FileChannel channel, ByteBuf buf, int buf_size) throws IOException {
        ByteBuffer buffer = buf.clear().nioReadBuffer(buf_size);
        if (Constants.CHECK_RANGE) {
            channel.read(buffer);
            if (buffer.hasRemaining()) {
                debug("read_not_complete: " + buf_size);
                for (; buffer.hasRemaining(); ) {
                    int read = channel.read(buffer);
                    if (read == -1) {
                        break;
                    }
                }
            }
        } else {
            channel.read(buffer);
        }
        buf.update_read();
    }

    public static long read_long(byte[] array, int from, int to) {
        long value = 0;
        int size = to - from;
        for (int i = 0; i < size; i++) {
            value = value * 10 + (array[from + i] - '0');
        }
        return value;
    }

    public static long read_long(long array, int from, int to) {
        long value = 0;
        long __from = array + from;
        int size = to - from;
        for (int i = 0; i < size; i++) {
            value = value * 10 + (unsafe.getByte(__from + i) - '0');
        }
        return value;
    }

    public static int index_of_n_forward(byte[] array, int index) {
        for (int i = 0; i < 41; i++) {
            if (array[index + i] == '\n') {
                return index + i;
            }
        }
        throw new RuntimeException("not found n");
    }

    public static int index_of_n_forward(long array, int index) {
        long __array = array + index;
        for (int i = 0; i < 41; i++) {
            if (unsafe.getByte(__array + i) == '\n') {
                return index + i;
            }
        }
        throw new RuntimeException("not found n");
    }

    public static int index_of_n(byte[] array, int index) {
        for (int i = 19; i >= 0; i--) {
            if (array[index + i] == '\n') {
                return index + i;
            }
        }
        throw new RuntimeException("not found n");
    }

    public static int index_of_n(long array, int index) {
        long __array = array + index;
        if (unsafe.getByte(__array + 19) == '\n') {
            return index + 19;
        }
        if (unsafe.getByte(__array + 18) == '\n') {
            return index + 18;
        }
        for (int i = 17; i >= 0; i--) {
            if (unsafe.getByte(__array + i) == '\n') {
                return index + i;
            }
        }
        throw new RuntimeException("not found n");
    }

    public static int index_of_comma(byte[] array, int index) {
        for (int i = 19; i >= 0; i--) {
            if (array[index + i] == ',') {
                return index + i;
            }
        }
        throw new RuntimeException("not found ,");
    }

    public static int index_of_comma(long array, int index) {
        long __array = array + index;
        if (unsafe.getByte(__array + 19) == ',') {
            return index + 19;
        }
        if (unsafe.getByte(__array + 18) == ',') {
            return index + 18;
        }
        for (int i = 17; i >= 0; i--) {
            if (unsafe.getByte(__array + i) == ',') {
                return index + i;
            }
        }
        throw new RuntimeException("not found ,");
    }

    public static void close(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static RandomAccessFile open_raf_read_only(Path path) throws FileNotFoundException {
        return new RandomAccessFile(path.toFile(), "r");
    }

    public static FileChannel open_read_only(Path path) throws IOException {
        return FileChannel.open(path, READ_ONLY);
    }

    public static FileChannel open_read_write(Path path) throws IOException {
        return FileChannel.open(path, READ_WRITE);
    }

    public static void print_jvm_args() {
        long mb = 1024 * 1024;
        long total = Runtime.getRuntime().totalMemory() / mb;
        long free = Runtime.getRuntime().freeMemory() / mb;
        long max = Runtime.getRuntime().maxMemory() / mb;
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        //        DebugUtil.info("Heap memory: {}", memoryBean.getHeapMemoryUsage());
        //        DebugUtil.info("Method memory: {}", memoryBean.getNonHeapMemoryUsage());
        debug("Jvm arguments:  " + inputArgs);
        debug("Total: " + total + ", free: " + free + ", max: " + max);
    }

    public static int index_of(long[] array, long value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return i;
            }
        }
        return -1;
    }

    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }

    private static sun.misc.Unsafe getUnsafe() {
        try {
            Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
            for (Field f : k.getDeclaredFields()) {
                f.setAccessible(true);
                Object x = f.get(null);
                if (k.isInstance(x)) {
                    return k.cast(x);
                }
            }
            // The sun.misc.Unsafe field does not exist.
            throw new Error("unsafe is null");
        } catch (Throwable e) {
            throw new Error("get unsafe failed", e);
        }
    }

    public static int log2(int num) {
        int ret = 0;
        int temp = num;
        for (; temp > 1; ) {
            temp >>>= 1;
            ret++;
        }
        return ret;
    }

    public static void copy_long_array(long[] src, long src_off, long[] dst, long dst_off, long len) {
        long src_address = Unsafe.ARRAY_LONG_BASE_OFFSET + (src_off << 3);
        long dst_address = Unsafe.ARRAY_LONG_BASE_OFFSET + (dst_off << 3);
        unsafe.copyMemory(src, src_address, dst, dst_address, len << 3);
    }

    public static ReadTask take_read_task(Queue<ReadTask> queue) throws InterruptedException {
        ReadTask task = queue.poll();
        if (task == null) {
            for (; ; ) {
                Thread.sleep(1);
                task = queue.poll();
                if (task != null) {
                    return task;
                }
            }
        }
        return task;
    }

    public static WriteTask take_write_task(Queue<WriteTask> queue) throws InterruptedException {
        WriteTask task = queue.poll();
        if (task == null) {
            for (; ; ) {
                Thread.sleep(1);
                task = queue.poll();
                if (task != null) {
                    return task;
                }
            }
        }
        return task;
    }

    public static ParseTask take_parse_task(Queue<ParseTask> queue) throws InterruptedException {
        ParseTask task = queue.poll();
        if (task == null) {
            for (; ; ) {
                Thread.sleep(1);
                task = queue.poll();
                if (task != null) {
                    return task;
                }
            }
        }
        return task;
    }

    public static ParseTask take_parse_task2(Queue<ParseTask> queue) throws InterruptedException {
        ParseTask task = queue.poll();
        if (task == null) {
            for (; ; ) {
                Thread.sleep(2);
                task = queue.poll();
                if (task != null) {
                    return task;
                }
            }
        }
        return task;
    }

}
