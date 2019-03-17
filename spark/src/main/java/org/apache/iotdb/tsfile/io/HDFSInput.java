package org.apache.iotdb.tsfile.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class HDFSInput implements TsFileInput {

  private FSDataInputStream fsDataInputStream;
  private FileStatus fileStatus;
  private boolean byteBufferReadable;

  public HDFSInput(String filePath) throws IOException {

    this(filePath, new Configuration());
  }

  public HDFSInput(String filePath, Configuration configuration) throws IOException {

    this(new Path(filePath), configuration);
  }

  public HDFSInput(Path path, Configuration configuration) throws IOException {
    FileSystem fs = path.getFileSystem(configuration);
    if(fs instanceof ChecksumFileSystem) {
      byteBufferReadable = false;
    } else {
      byteBufferReadable = true;
    }
    fsDataInputStream = fs.open(path);
    fileStatus = fs.getFileStatus(path);
  }

  @Override
  public long size() {
    return fileStatus.getLen();
  }

  @Override
  public long position() throws IOException {
    return fsDataInputStream.getPos();
  }

  @Override
  public TsFileInput position(long newPosition) throws IOException {
    fsDataInputStream.seek(newPosition);
    return this;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int res;
    if(byteBufferReadable) {
      res = fsDataInputStream.read(dst);
    } else {
      byte[] bytes = new byte[dst.remaining()];
      res = fsDataInputStream.read(bytes);
      dst.put(bytes);
    }
    return res;
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("position must be non-negative");
    }

    if (position >= this.size()) {
      return -1;
    }

    long srcPosition = fsDataInputStream.getPos();

    fsDataInputStream.seek(position);

    int res;
    if(byteBufferReadable) {
      res = fsDataInputStream.read(dst);
    } else {
      byte[] bytes = new byte[dst.remaining()];
      res = fsDataInputStream.read(bytes);
      dst.put(bytes);
    }

    fsDataInputStream.seek(srcPosition);

    return res;
  }

  @Override
  public int read() throws IOException {
    throw new IOException("Not support");
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    throw new IOException("Not support");
  }

  @Override
  public FileChannel wrapAsFileChannel() throws IOException {
    throw new IOException("Not support");
  }

  @Override
  public InputStream wrapAsInputStream() throws IOException {
    return fsDataInputStream;
  }

  @Override
  public void close() throws IOException {
    fsDataInputStream.close();
  }

  @Override
  public int readInt() throws IOException {
    throw new IOException("Not support");
  }

}
