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

import org.jtransforms.fft.DoubleFFT_1D;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class FreqDecoderTest {

  private static final int ROW_NUM = 1024;
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
      data[i] = Math.cos(0.25 * Math.PI * i);
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertTrue(SNR(data, recover, ROW_NUM) > 40);
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
    assertTrue(SNR(data, recover, ROW_NUM) > 40);
  }

  @Test
  public void testMaxMin() throws IOException {
    reader.reset();
    double[] data = new double[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = (i & 1) == 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertTrue(SNR(data, recover, ROW_NUM) > 40);
  }

  @Test
  public void testConstantInt() throws IOException {
    reader.reset();
    double[] data = new double[ROW_NUM];
    for (int i = 0; i < 10; i++) {
      constantInt(i, data);
    }
  }

  private void boundInt(int power, double[] data) throws IOException {
    reader.reset();
    double maxn = 1 << power;
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = ran.nextDouble() * maxn;
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertTrue(SNR(data, recover, ROW_NUM) > 40);
  }

  private void constantInt(int value, double[] data) throws IOException {
    reader.reset();
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = value;
    }
    double[] recover = shouldReadAndWrite(data, ROW_NUM);
    assertTrue(SNR(data, recover, ROW_NUM) > 40);
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

  public double SNR(double[] gd, double[] x, int length) {
    double noise_power = 0, signal_power = 0;
    for (int i = 0; i < length; i++) {
      noise_power += (gd[i] - x[i]) * (gd[i] - x[i]);
      signal_power += gd[i] * gd[i];
    }
    if (noise_power == 0) {
      return Double.POSITIVE_INFINITY;
    } else {
      return 10 * Math.log10(signal_power / noise_power);
    }
  }
}
