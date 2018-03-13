package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangjinrui on 2018/1/13.
 */
public class ConcurrentRandomAccessFile implements ITsRandomAccessFileReader {

    private RandomAccessFile randomAccessFile;

    private ThreadLocal<Long> offset;
    private AtomicLong lastReadOffset;
    private boolean isClosed;
    private Object readLock;

    public ConcurrentRandomAccessFile(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
        readLock = new Object();
        offset = new ThreadLocal<>();
        lastReadOffset = new AtomicLong(0);
        isClosed = false;
    }

    @Override
    public void seek(long offset) throws IOException {
        this.offset.set(offset);
    }

    @Override
    public int read() throws IOException {
        synchronized (readLock) {
            checkOffset();
            offset.set(offset.get() + 1);
            lastReadOffset.set(offset.get());
            return randomAccessFile.read();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        synchronized (readLock) {
            checkOffset();
            int end = len + off;
            int get;
            int total = 0;
            for (int i = off; i < end; i += get) {
                get = randomAccessFile.read(b, i, end - i);
                if (get > 0)
                    total += get;
                else
                    break;
            }
            offset.set(offset.get() + total);
            lastReadOffset.set(offset.get());
            return total;
        }
    }

    @Override
    public long length() throws IOException {
        return randomAccessFile.length();
    }

    @Override
    public int readInt() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        synchronized (readLock) {
            if (!isClosed) {
                randomAccessFile.close();
                isClosed = true;
            }
        }
    }

    private void checkOffset() throws IOException {
        if (offset.get() == null) {
            offset.set(0L);
        }
        if (offset.get() != lastReadOffset.get()) {
            randomAccessFile.seek(offset.get());
        }
    }
}
