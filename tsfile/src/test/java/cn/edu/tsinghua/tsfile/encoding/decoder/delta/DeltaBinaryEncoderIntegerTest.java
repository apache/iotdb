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
public class DeltaBinaryEncoderIntegerTest {
  private DeltaBinaryEncoder writer;
  private DeltaBinaryDecoder reader;
  private static final int ROW_NUM = 10000;

  private Random ran = new Random();
  ByteArrayOutputStream out;

  @Before
  public void test() {
    writer = new DeltaBinaryEncoder.IntDeltaEncoder();
    reader = new DeltaBinaryDecoder.IntDeltaDecoder();
  }

  @Test
  public void testBasic() throws IOException {
    System.out.println("write basic");
    int data[] = new int[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++)
      data[i] = i * i;
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testBoundInt() throws IOException {
    System.out.println("write bounded int");
    int data[] = new int[ROW_NUM];
    for (int i = 0; i < 10; i++) {
      boundInt(i, data);
    }
  }

  private void boundInt(int power, int[] data) throws IOException {
    System.out.println("the bound of 2 power:" + power);
    for (int i = 0; i < ROW_NUM; i++)
      data[i] = ran.nextInt((int) Math.pow(2, power));
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRandom() throws IOException {
    System.out.println("write random");
    int data[] = new int[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++)
      data[i] = ran.nextInt();
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testMaxMin() throws IOException {
    System.out.println("write maxmin");
    int data[] = new int[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++)
      data[i] = (i & 1) == 0 ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    shouldReadAndWrite(data, ROW_NUM);
  }

  private void writeData(int[] data, int length) throws IOException {
    for (int i = 0; i < length; i++) {
      writer.encode(data[i], out);
    }
    writer.flush(out);
  }

  private ByteArrayInputStream in;

  private void shouldReadAndWrite(int[] data, int length) throws IOException {
    System.out.println("source data size:" + 4 * length + " byte");
    out = new ByteArrayOutputStream();
    writeData(data, length);
    byte[] page = out.toByteArray();
    System.out.println("encoding data size:" + page.length + " byte");
    in = new ByteArrayInputStream(page);
    int i = 0;
    while (reader.hasNext(in)) {
      assertEquals(data[i++], reader.readInt(in));
    }
  }

}

