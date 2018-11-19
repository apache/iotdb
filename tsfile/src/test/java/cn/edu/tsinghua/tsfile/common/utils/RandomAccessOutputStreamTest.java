package cn.edu.tsinghua.tsfile.common.utils;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.junit.Test;

public class RandomAccessOutputStreamTest {
  private static final String fileName = "testRandomAccessOutputStream";

  @Test
  public void testRandomAccessOutputStreamFile() throws IOException {
    byte bt1 = 1;
    int i1 = 123123;
    boolean b1 = false;
    long l1 = 134578845l;
    float f1 = 13547.2376f;
    double d1 = 12123.158476d;
    File file = new File(fileName);
    if (file.exists())
      file.delete();
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    out.write(bt1);
    out.write(BytesUtils.intToBytes(i1));
    out.write(BytesUtils.boolToBytes(b1));
    out.write(BytesUtils.longToBytes(l1));
    out.write(BytesUtils.floatToBytes(f1));
    out.write(BytesUtils.doubleToBytes(d1));
    assertEquals(1 + 4 + 1 + 8 + 4 + 8, out.getPos());
    OutputStream retOut = out.getOutputStream();
    retOut.flush();
    out.close();
    // test correctness
    RandomAccessFile input = new RandomAccessFile(fileName, "r");
    assertEquals(bt1, input.read());
    assertEquals(i1, input.readInt());
    assertEquals(b1, input.readBoolean());
    assertEquals(l1, input.readLong());
    assertEquals(f1, input.readFloat(), CommonTestConstant.float_min_delta);
    assertEquals(d1, input.readDouble(), CommonTestConstant.double_min_delta);
    input.close();
    file.delete();
  }
}
