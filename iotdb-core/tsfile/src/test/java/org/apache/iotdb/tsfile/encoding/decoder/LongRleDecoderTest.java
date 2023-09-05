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

import org.apache.iotdb.tsfile.encoding.encoder.LongRleEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.RleEncoder;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LongRleDecoderTest {

  private List<Long> rleList;
  private List<Long> bpList;
  private List<Long> hybridList;
  private int rleBitWidth;
  private int bpBitWidth;
  private int hybridWidth;

  @Before
  public void setUp() {
    rleList = new ArrayList<Long>();
    int rleCount = 11;
    int rleNum = 38;
    long rleStart = 11;
    for (int i = 0; i < rleNum; i++) {
      for (int j = 0; j < rleCount; j++) {
        rleList.add(rleStart);
      }
      for (int j = 0; j < rleCount; j++) {
        rleList.add(rleStart - 1);
      }
      rleCount += 2;
      rleStart *= -3;
    }
    rleBitWidth = ReadWriteForEncodingUtils.getLongMaxBitWidth(rleList);

    bpList = new ArrayList<Long>();
    int bpCount = 15;
    long bpStart = 11;
    for (int i = 0; i < bpCount; i++) {
      bpStart *= 3;
      if (i % 2 == 1) {
        bpList.add(bpStart * -1);
      } else {
        bpList.add(bpStart);
      }
    }
    bpBitWidth = ReadWriteForEncodingUtils.getLongMaxBitWidth(bpList);

    hybridList = new ArrayList<Long>();
    int hybridCount = 11;
    int hybridNum = 1000;
    long hybridStart = 20;

    for (int i = 0; i < hybridNum; i++) {
      for (int j = 0; j < hybridCount; j++) {
        hybridStart += 3;
        if (j % 2 == 1) {
          hybridList.add(hybridStart * -1);
        } else {
          hybridList.add(hybridStart);
        }
      }
      for (int j = 0; j < hybridCount; j++) {
        if (i % 2 == 1) {
          hybridList.add(hybridStart * -1);
        } else {
          hybridList.add(hybridStart);
        }
      }
      hybridCount += 2;
    }

    hybridWidth = ReadWriteForEncodingUtils.getLongMaxBitWidth(hybridList);
  }

  @After
  public void tearDown() {}

  @Test
  public void testRleReadBigLong() throws IOException {
    List<Long> list = new ArrayList<>();
    for (long i = 8000000; i < 8400000; i++) {
      list.add(i);
    }
    int width = ReadWriteForEncodingUtils.getLongMaxBitWidth(list);
    testLength(list, false, 1);
    for (int i = 1; i < 10; i++) {
      testLength(list, false, i);
    }
  }

  @Test
  public void testRleReadLong() throws IOException {
    for (int i = 1; i < 2; i++) {
      testLength(rleList, false, i);
    }
  }

  @Test
  public void testMaxRLERepeatNUM() throws IOException {
    List<Long> repeatList = new ArrayList<>();
    int rleCount = 17;
    int rleNum = 5;
    long rleStart = 11;
    for (int i = 0; i < rleNum; i++) {
      for (int j = 0; j < rleCount; j++) {
        repeatList.add(rleStart);
      }
      for (int j = 0; j < rleCount; j++) {
        repeatList.add(rleStart / 3);
      }
      rleCount *= 7;
      rleStart *= -3;
    }
    int bitWidth = ReadWriteForEncodingUtils.getLongMaxBitWidth(repeatList);
    for (int i = 1; i < 10; i++) {
      testLength(repeatList, false, i);
    }
  }

  @Test
  public void testBitPackingReadLong() throws IOException {
    for (int i = 1; i < 10; i++) {
      testLength(bpList, false, i);
    }
  }

  @Test
  public void testHybridReadLong() throws IOException {
    for (int i = 1; i < 10; i++) {
      testLength(hybridList, false, i);
    }
  }

  @Test
  public void testBitPackingReadHeader() throws IOException {
    for (int i = 1; i < 505; i++) {
      testBitPackedReadHeader(i);
    }
  }

  private void testBitPackedReadHeader(int num) throws IOException {
    List<Long> list = new ArrayList<Long>();

    for (long i = 0; i < num; i++) {
      list.add(i);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int bitWidth = ReadWriteForEncodingUtils.getLongMaxBitWidth(list);
    RleEncoder<Long> encoder = new LongRleEncoder();
    for (long value : list) {
      encoder.encode(value, baos);
    }
    encoder.flush(baos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ReadWriteForEncodingUtils.readUnsignedVarInt(bais);
    assertEquals(bitWidth, bais.read());
    int header = ReadWriteForEncodingUtils.readUnsignedVarInt(bais);
    int group = header >> 1;
    assertEquals(group, (num + 7) / 8);
    int lastBitPackedNum = bais.read();
    if (num % 8 == 0) {
      assertEquals(lastBitPackedNum, 8);
    } else {
      assertEquals(lastBitPackedNum, num % 8);
    }
  }

  public void testLength(List<Long> list, boolean isDebug, int repeatCount) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RleEncoder<Long> encoder = new LongRleEncoder();
    for (int i = 0; i < repeatCount; i++) {
      for (long value : list) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    RleDecoder decoder = new LongRleDecoder();
    for (int i = 0; i < repeatCount; i++) {
      for (long value : list) {
        long value_ = decoder.readLong(buffer);
        if (isDebug) {
          System.out.println(value_ + "/" + value);
        }
        assertEquals(value, value_);
      }
    }
  }
}
