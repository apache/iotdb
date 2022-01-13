/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.FreqEncoder;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.jtransforms.fft.DoubleFFT_1D;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class FreqDecoderTest {

  private static final int ROW_NUM = 10000;
  ByteArrayOutputStream out;
  private FreqEncoder writer;
  private FreqDecoder reader;
  private final Random ran = new Random();
  private ByteBuffer buffer;

  @Before
  public void test() {
    writer = new FreqEncoder();
    reader = new FreqDecoder();
  }

  @Test
  public void testSin() throws IOException {
    reader.reset();
    double[] data = new double[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = Math.sin(0.25 * Math.PI * i);
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertEquals(0, rmse(data, recover, ROW_NUM), 1e-10);
  }

  @Test
  public void testBoundInt() throws IOException {
    reader.reset();
    double[] data = new double[ROW_NUM];
    for (int i = 2; i < 21; i++) {
      boundInt(i, data);
    }
  }

  @Test
  public void testLongTail() throws IOException {
    reader.reset();
    double[] a = new double[ROW_NUM * 2];
    for (int i = 0; i < ROW_NUM; i++) {
      double amp = Math.exp(-i);
      double theta = ran.nextDouble() * 2 * Math.PI;
      a[i * 2] = amp * Math.cos(theta);
      a[i * 2 + 1] = amp * Math.sin(theta);
    }
    DoubleFFT_1D fft = new DoubleFFT_1D(ROW_NUM);
    fft.complexInverse(a, false);
    double data[] = new double[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = a[i * 2];
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertEquals(0, rmse(data, recover, ROW_NUM), 0.02);
  }

  @Test
  public void testMaxMin() throws IOException {
    reader.reset();
    double[] data = new double[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = (i & 1) == 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertEquals(0, rmse(data, recover, ROW_NUM), 0.01 * Long.MAX_VALUE);
  }

  @Test
  public void testConstantInt() throws IOException {
    reader.reset();
    double[] data = new double[ROW_NUM];
    for (int i = 0; i < 10; i++) {
      constantInt(i, data);
    }
  }

  @Test
  public void testRealData() throws IOException {
    reader.reset();
    for (int len = 10_0000; len <= 100_0000; len += 10_0000) {
      double data[] = readFromFile("D:\\科研_THU\\Tools\\MavenTool\\data3.csv", len);
      double[] recover = shouldReadAndWrite(data, len);
      assertEquals(0, rmse(data, recover, len), 0.25);
    }
  }

  private double[] readFromFile(String name, int totalNum) {
    DoubleArrayList origin = new DoubleArrayList(totalNum);
    try {
      Scanner sc = new Scanner(new File(name));
      sc.useDelimiter("\\s*(,|\\r|\\n)\\s*"); // 设置分隔符，以逗号或回车分隔，前后可以有若干个空白符
      while (sc.hasNext()) {
        double x = sc.nextDouble();
        origin.add(x);
        if (origin.size() >= totalNum) {
          break;
        }
      }
      int m = origin.size();
      Random random = new Random();
      for (int i = m; i < totalNum; i++) {
        origin.add(origin.get(i % m) + random.nextGaussian());
      }
      sc.close();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(FreqDecoderTest.class.getName()).log(Level.SEVERE, null, ex);
    }
    return origin.toArray();
  }

  private void boundInt(int power, double[] data) throws IOException {
    reader.reset();
    double maxn = 1 << power;
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = ran.nextDouble() * maxn;
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertEquals(0, rmse(data, recover, ROW_NUM), 0.02 * maxn);
  }

  private void constantInt(int value, double[] data) throws IOException {
    reader.reset();
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = value;
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertEquals(0, rmse(data, recover, ROW_NUM), 1e-10);
  }

  private void writeData(double[] data, int length) {
    for (int i = 0; i < length; i++) {
      writer.encode(data[i], out);
    }
    writer.flush(out);
  }

  private double[] shouldReadAndWrite(double[] data, int length) throws IOException {
    out = new ByteArrayOutputStream();
    writeData(data, length);
    byte[] page = out.toByteArray();
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    double recover[] = new double[length];
    while (reader.hasNext(buffer)) {
      recover[i] = reader.readDouble(buffer);
      i++;
    }
    return recover;
  }

  public double rmse(double x1[], double x2[], int length) {
    double sum = 0;
    for (int i = 0; i < length; i++) {
      sum += (x1[i] - x2[i]) * (x1[i] - x2[i]);
    }
    return Math.sqrt(sum / (length - 1));
  }
}
