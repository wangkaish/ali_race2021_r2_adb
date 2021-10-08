package com.aliyun.adb.contest.common;

import com.aliyun.adb.contest.task.WriteTask;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

/**
 * @author: wangkai
 * @create: 2021-06-30 17:27
 **/
public class ByteBuf extends Constants {

    static final Unsafe unsafe = Util.unsafe;

    public static final Constructor<ByteBuffer> DIRECT_BYTE_BUFFER_CONSTRUCT = get_direct_byte_buffer_construct();

    @SuppressWarnings("unchecked")
    private static Constructor<ByteBuffer> get_direct_byte_buffer_construct() {
        try {
            Class<?> clazz = Class.forName("java.nio.DirectByteBuffer");
            Constructor<?> constructor = clazz.getDeclaredConstructor(long.class, int.class);
            constructor.setAccessible(true);
            return (Constructor<ByteBuffer>) constructor;
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public static ByteBuffer construct_byte_buf(long address, int cap) {
        try {
            return DIRECT_BYTE_BUFFER_CONSTRUCT.newInstance(address, cap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public WriteTask task;

    public final ByteBuffer buf;
    public final long address;
    public final long offset;
    public final long limit;

    public long read_index;
    public long write_index;

    public static ByteBuf allocate(int cap) {
        return new ByteBuf(cap);
    }

    private ByteBuf(int cap) {
        this.address = unsafe.allocateMemory(cap);
        if (address == -1) {
            throw new RuntimeException("Allocate memory failed: " + cap);
        }
        this.buf = construct_byte_buf(address, cap);
        this.offset = address;
        this.limit = address + cap;
        this.clear();
    }

    private ByteBuf(ByteBuffer buf, long address, long offset, long limit) {
        this.buf = buf;
        this.address = address;
        this.offset = offset;
        this.limit = limit;
        this.clear();
    }

    public void write_long(long value) {
        unsafe.putLong(write_index, value);
        write_index += 8;
    }

    public long read_long() {
        long i = read_index;
        read_index += 8;
        return unsafe.getLong(i);
    }

    public long get_long(int index) {
        return unsafe.getLong(offset + (index));
    }

    public void get_long(long[] array, int array_off, long from, long end) {
        int j = array_off;
        long f = offset + (from << 3);
        long t = offset + (end << 3);
        for (long i = f; i < t; ) {
            array[j++] = unsafe.getLong(i);
            i += 8;
        }
    }

    public void set_long(int index, long value) {
        unsafe.putLong(offset + index, value);
    }

    public boolean is_readable() {
        return read_index < write_index;
    }

    public boolean is_writable() {
        return write_index < limit;
    }

    public long write_remain() {
        return limit - write_index;
    }

    public long read_remain() {
        return write_index - read_index;
    }

    public ByteBuf clear() {
        read_index = offset;
        write_index = offset;
        return this;
    }

    public ByteBuf slice(int off, int len) {
        ByteBuffer buffer = buf.duplicate();
        long offset = address + off;
        long limit = offset + len;
        return new ByteBuf(buffer, address, offset, limit);
    }

    public ByteBuffer nioWriteBuffer() {
        int __limit = (int) (write_index - address);
        int __pos = (int) (read_index - address);
        try {
            buf.clear().limit(__limit).position(__pos);
        } catch (Exception e) {
            Util.debug("buf: " + buf + ", this: " + this + ", __limit: " + __limit + ", __pos: " + __pos);
            throw e;
        }
        return buf;
    }

    public ByteBuffer nioReadBuffer(int limit) {
        int __limit = (int) (offset - address + limit);
        int __position = (int) (offset - address);
        try {
            buf.clear().limit(__limit).position(__position);
        } catch (Exception e) {
            Util.debug("buf: " + buf + ", this: " + this + ", __limit: " + __limit + ", limit: " + limit);
            throw e;
        }
        return buf;
    }

    public void update_read() {
        this.read_index = offset;
        this.write_index = address + buf.position();
    }

    @Override
    public String toString() {
        return "ByteBuf{off:" + (offset - address)
                + ", rdx: " + (read_index - address)
                + ", wdx: " + (write_index - address)
                + ", read_remain: " + read_remain()
                + ", write_remain: " + write_remain()
                + ", cap: " + (limit - offset) + "}";
    }
}
