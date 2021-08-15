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
package org.apache.iotdb.tsfile.encoding.decoder.delta;

import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class DeltaBinaryEncoderIntegerTest {

  private static final int ROW_NUM = 10000;
  ByteArrayOutputStream out;
  private DeltaBinaryEncoder writer;
  private DeltaBinaryDecoder reader;
  private Random ran = new Random();
  private ByteBuffer buffer;

  @Before
  public void test() {
    writer = new DeltaBinaryEncoder.IntDeltaEncoder();
    reader = new DeltaBinaryDecoder.IntDeltaDecoder();
  }

  @Test
  public void testBasic() throws IOException {
    int[] data = new int[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = i * i;
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testBoundInt() throws IOException {
    int[] data = new int[ROW_NUM];
    for (int i = 0; i < 10; i++) {
      boundInt(i, data);
    }
  }

  private void boundInt(int power, int[] data) throws IOException {
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = ran.nextInt((int) Math.pow(2, power));
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRandom() throws IOException {
    int[] data = new int[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = ran.nextInt();
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testMaxMin() throws IOException {
    int[] data = new int[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = (i & 1) == 0 ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  private void writeData(int[] data, int length) {
    for (int i = 0; i < length; i++) {
      writer.encode(data[i], out);
    }
    writer.flush(out);
  }

  private void shouldReadAndWrite(int[] data, int length) throws IOException {
    // System.out.println("source data size:" + 4 * length + " byte");
    out = new ByteArrayOutputStream();
    writeData(data, length);
    byte[] page = out.toByteArray();
    // System.out.println("encoding data size:" + page.length + " byte");
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (reader.hasNext(buffer)) {
      assertEquals(data[i++], reader.readInt(buffer));
    }
  }
}
