package cn.edu.tsinghua.tsfile.common.utils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * TSRandomAccessFileWriter is an interface for TSFile writer. Each output should implements this
 * interface whatever os file system or HDFS.<br>
 * The main difference between RandomAccessOutputStream and general OutputStream
 * is:RandomAccessOutputStream provide method {@code getPos} for random accessing. It also
 * implements {@code getOutputStream} to return an OutputStream supporting tsfile-format
 *
 * @author kangrong
 */
public interface ITsRandomAccessFileWriter {
    long getPos() throws IOException;

    void seek(long offset) throws IOException;

    void write(byte[] b) throws IOException;

    void write(int b) throws IOException;

    void close() throws IOException;

    OutputStream getOutputStream();
}
