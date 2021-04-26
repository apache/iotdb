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
package org.apache.iotdb.tsfile.encoding.decoder.regular;

import org.apache.iotdb.tsfile.encoding.decoder.RegularDataDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.RegularDataEncoder;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class RegularDataEncoderIntegerTest {

  private static int ROW_NUM;
  ByteArrayOutputStream out;
  private RegularDataEncoder regularDataEncoder;
  private RegularDataDecoder regularDataDecoder;
  private ByteBuffer buffer;

  @Before
  public void test() {
    regularDataEncoder = new RegularDataEncoder.IntRegularEncoder();
    regularDataDecoder = new RegularDataDecoder.IntRegularDecoder();
  }

  @Test
  public void testRegularEncodingWithoutMissingPoint() throws IOException {
    ROW_NUM = 2000000;

    int[] data = new int[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = i;
    }

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithOnePercentMissingPoints() throws IOException {
    int[] data = getMissingPointData(2000000, 80);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithFivePercentMissingPoints() throws IOException {
    int[] data = getMissingPointData(2000000, 20);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithTenPercentMissingPoints() throws IOException {
    int[] data = getMissingPointData(2000000, 10);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithTwentyPercentMissingPoints() throws IOException {
    int[] data = getMissingPointData(2000000, 5);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithLowMissingPoints1() throws IOException {
    int[] data = getMissingPointData(2000000, 1700);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithLowMissingPoints2() throws IOException {
    int[] data = getMissingPointData(2000000, 40000);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testMissingPointsDataSize() throws IOException {
    int[] originalData = new int[] {1000, 1100, 1200, 1300, 1500, 2000};
    out = new ByteArrayOutputStream();
    writeData(originalData, 6);
    byte[] page = out.toByteArray();
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (regularDataDecoder.hasNext(buffer)) {
      assertEquals(originalData[i++], regularDataDecoder.readInt(buffer));
    }
  }

  private int[] getMissingPointData(int dataSize, int missingPointInterval) {
    ROW_NUM = dataSize;

    int originalRowNum = ROW_NUM;

    int kong = 0;
    for (int i = 0; i < ROW_NUM; i++) {
      if (i % missingPointInterval == 0) {
        kong++;
      }
    }

    ROW_NUM = originalRowNum - kong;

    int[] data = new int[ROW_NUM];
    int j = 0;
    for (int i = 0; i < originalRowNum; i++) {
      if (i % missingPointInterval == 0) {
        continue;
      }
      data[j++] = i;
    }

    return data;
  }

  private void writeData(int[] data, int length) {
    for (int i = 0; i < length; i++) {
      regularDataEncoder.encode(data[i], out);
    }
    regularDataEncoder.flush(out);
  }

  private void shouldReadAndWrite(int[] data, int length) throws IOException {
    out = new ByteArrayOutputStream();
    writeData(data, length);
    byte[] page = out.toByteArray();
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (regularDataDecoder.hasNext(buffer)) {
      assertEquals(data[i++], regularDataDecoder.readInt(buffer));
    }
  }
}
