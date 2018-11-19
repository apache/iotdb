package cn.edu.tsinghua.iotdb.queryV2.engine.reader.component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

/**
 * <p>
 * An implements of InputStream which can reduce the total amount of opened files in one thread.
 * </p>
 * <p>
 * IMPORTANT: Multiple SegmentInputStream with same RandomAccessFile reference could not be used in different thread.
 * </p>
 * Created by zhangjinrui on 2018/1/14.
 */
public class SegmentInputStreamWithMMap extends SegmentInputStream {

    private RandomAccessFile randomAccessFile;
    private long offset;
    private long position;
    private long size;
    private long mark;

    // NIO MMAP
    private MappedByteBuffer mmap;
    private ByteArrayInputStream byteArrayInputStream;

    public SegmentInputStreamWithMMap(RandomAccessFile randomAccessFile, long offset, long size) throws IOException {
        this.randomAccessFile = randomAccessFile;
        //mmap = this.randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, size);
        this.offset = offset;
        this.size = size;
        this.position = offset;
        this.mark = offset;
    }

    public SegmentInputStreamWithMMap(MappedByteBuffer mmap, long offset, long size) throws IOException {
        this.mmap = mmap;
        this.offset = offset;
        this.size = size;
        this.position = offset;
        this.mark = offset;
    }

    @Override
    public int read() throws IOException {
        if (position >= offset + size) {
            return -1;
        }
        int b = mmap.get((int)position);
        position += 1;
        return b;
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
        checkPosition();

        for (int i = 0; i < length; i++) {
            b[i] = mmap.get((int) position + offset + i);
        }

        if (mmap.hasRemaining()) {
            position += length;
            return length;
        }
        return -1;

//        checkPosition();
//
//        int total = randomAccessFile.read(b, offset, length);
//        position += total;
//        return total;
    }

    public long skip(long n) {
        if (n <= 0) {
            return 0;
        }
        if (position + n >= offset + size) {
            long skipped = offset + size - position;
            position = offset + size;
            return skipped;
        } else {
            position += n;
            return n;
        }
    }

    public boolean markSupported() {
        return true;
    }

    public synchronized void mark(int readlimit) {
        mark = readlimit;
    }

    public synchronized void reset() {
        position = mark;
    }

    public int available() {
        int left = (int) (offset + size - position);
        return left > 0 ? left : 0;
    }

    private void checkPosition() throws IOException {
        if (position >= offset + size) {
            throw new IOException("no available byte in current stream");
        }
    }
}