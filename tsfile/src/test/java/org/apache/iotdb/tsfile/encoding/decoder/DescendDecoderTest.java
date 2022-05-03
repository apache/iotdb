/*
 * Copyright 2022 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.DescendEncoder;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class DescendDecoderTest {

  private static final int ROW_NUM = 1024;
  ByteArrayOutputStream out;
  private DescendEncoder writer;
  private DescendDecoder reader;
  private final Random ran = new Random();
  private ByteBuffer buffer;
  private double delta = 1e-10;

  @Before
  public void test() {
    writer = new DescendEncoder();
    reader = new DescendDecoder();
  }

  @Test
  public void testBasic() throws IOException {
    double[] data = new double[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = i * i;
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testBoundInt() throws IOException {
    double[] data = new double[ROW_NUM];
    for (int i = 0; i < 10; i++) {
      boundInt(i, data);
    }
  }

  private void boundInt(int power, double[] data) throws IOException {
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = ran.nextInt((int) Math.pow(2, power));
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRandom() throws IOException {
    double[] data = new double[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = ran.nextInt(0x7fffffff);
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRandomWithBeta() throws IOException {
    double[] data = new double[ROW_NUM];
    for (int beta = -10; beta <= 10; beta++) {
      for (int i = 0; i < ROW_NUM; i++) {
        data[i] = ran.nextInt(0x7fff) * Math.pow(2, beta);
      }
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  private void shouldReadAndWrite(double[] data, int length) throws IOException {
    // System.out.println("source data size:" + 4 * length + " byte");
    out = new ByteArrayOutputStream();
    writeData(data, length);
    byte[] page = out.toByteArray();
    // System.out.println("encoding data size:" + page.length + " byte");
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (reader.hasNext(buffer)) {
      assertEquals(data[i++], reader.readInt(buffer), delta);
    }
  }

  private void writeData(double[] data, int length) {
    for (int i = 0; i < length; i++) {
      writer.encode(data[i], out);
    }
    writer.flush(out);
  }
}
