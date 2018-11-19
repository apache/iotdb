package cn.edu.tsinghua.tsfile.timeseries.read;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;

/**
 * File-read interface for local file.
 *
 * @author Jinrui Zhang
 */
public class TsRandomAccessLocalFileReader implements ITsRandomAccessFileReader {

  private RandomAccessFile raf;

  public TsRandomAccessLocalFileReader(String filePath) throws FileNotFoundException {
    this.raf = new RandomAccessFile(filePath, "r");
  }

  public TsRandomAccessLocalFileReader(RandomAccessFile raf) {
    this.raf = raf;
  }

  @Override
  public void seek(long offset) throws IOException {
    this.raf.seek(offset);
  }

  @Override
  public int read() throws IOException {
    return raf.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int end = len + off;
    int get = 1;
    int total = 0;
    for (int i = off; i < end; i += get) {
      get = raf.read(b, i, end - i);
      if (get > 0)
        total += get;
      else
        break;
    }
    return total;
  }

  @Override
  public long length() throws IOException {
    return raf.length();
  }

  @Override
  public int readInt() throws IOException {
    return raf.readInt();
  }

  @Override
  public void close() throws IOException {
    raf.close();
  }
}
