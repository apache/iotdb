package cn.edu.tsinghua.tsfile.encoding.decoder.delta;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import cn.edu.tsinghua.tsfile.encoding.decoder.DeltaBinaryDecoder;
import cn.edu.tsinghua.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author kangrong
 *
 */
public class DeltaBinaryEncoderLongTest {
  private DeltaBinaryEncoder writer;
  private DeltaBinaryDecoder reader;
  private static final int ROW_NUM = 10000;
  private Random ran = new Random();
  private final long BASIC_FACTOR = 1l << 32;
  ByteArrayOutputStream out;

  @Before
  public void test() {
    writer = new DeltaBinaryEncoder.LongDeltaEncoder();
    reader = new DeltaBinaryDecoder.LongDeltaDecoder();
  }

  @Test
  public void testBasic() throws IOException {
    System.out.println("write basic");
    long data[] = new long[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++)
      data[i] = i * i * BASIC_FACTOR;
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testBoundInt() throws IOException {
    System.out.println("write bounded int");
    long data[] = new long[ROW_NUM];
    for (int i = 2; i < 21; i++) {
      boundInt(i, data);
    }
  }

  private void boundInt(int power, long[] data) throws IOException {
    System.out.println("the bound of 2 power:" + power);
    for (int i = 0; i < ROW_NUM; i++)
      data[i] = ran.nextInt((int) Math.pow(2, power)) * BASIC_FACTOR;
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRandom() throws IOException {
    System.out.println("write random");
    long data[] = new long[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++)
      data[i] = ran.nextLong();
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testMaxMin() throws IOException {
    System.out.println("write maxmin");
    long data[] = new long[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++)
      data[i] = (i & 1) == 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
    shouldReadAndWrite(data, ROW_NUM);
  }

  private void writeData(long[] data, int length) throws IOException {
    for (int i = 0; i < length; i++) {
      writer.encode(data[i], out);
    }
    writer.flush(out);
  }

  private ByteArrayInputStream in;

  private void shouldReadAndWrite(long[] data, int length) throws IOException {
    System.out.println("source data size:" + 4 * length + " byte");
    out = new ByteArrayOutputStream();
    writeData(data, length);
    byte[] page = out.toByteArray();
    System.out.println("encoding data size:" + page.length + " byte");
    in = new ByteArrayInputStream(page);
    int i = 0;
    while (reader.hasNext(in)) {
      assertEquals(data[i++], reader.readLong(in));
    }
  }

}
