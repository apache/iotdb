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

import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoderV1;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.SinglePrecisionEncoderV1;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GorillaDecoderV1Test {

  private static final Logger logger = LoggerFactory.getLogger(GorillaDecoderV1Test.class);
  private final double delta = 0.0000001;
  private final int floatMaxPointValue = 10000;
  private final long doubleMaxPointValue = 1000000000000000L;
  private List<Float> floatList;
  private List<Double> doubleList;

  @Before
  public void setUp() {
    floatList = new ArrayList<Float>();
    int hybridCount = 11;
    int hybridNum = 50;
    int hybridStart = 2000;
    for (int i = 0; i < hybridNum; i++) {
      for (int j = 0; j < hybridCount; j++) {
        floatList.add((float) hybridStart / floatMaxPointValue);
      }
      for (int j = 0; j < hybridCount; j++) {
        floatList.add((float) hybridStart / floatMaxPointValue);
        hybridStart += 3;
      }

      hybridCount += 2;
    }

    doubleList = new ArrayList<Double>();
    int hybridCountDouble = 11;
    int hybridNumDouble = 50;
    long hybridStartDouble = 2000;

    for (int i = 0; i < hybridNumDouble; i++) {
      for (int j = 0; j < hybridCountDouble; j++) {
        doubleList.add((double) hybridStartDouble / doubleMaxPointValue);
      }
      for (int j = 0; j < hybridCountDouble; j++) {
        doubleList.add((double) hybridStartDouble / doubleMaxPointValue);
        hybridStart += 3;
      }

      hybridCountDouble += 2;
    }
  }

  @After
  public void tearDown() {}

  @Test
  public void testNegativeNumber() throws IOException {
    Encoder encoder = new SinglePrecisionEncoderV1();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    float value = -7.101f;
    encoder.encode(value, baos);
    encoder.encode(value - 2, baos);
    encoder.encode(value - 4, baos);
    encoder.flush(baos);
    encoder.encode(value, baos);
    encoder.encode(value - 2, baos);
    encoder.encode(value - 4, baos);
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < 2; i++) {
      Decoder decoder = new SinglePrecisionDecoderV1();
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readFloat(buffer), delta);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value - 2, decoder.readFloat(buffer), delta);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value - 4, decoder.readFloat(buffer), delta);
      }
    }
  }

  @Test
  public void testZeroNumber() throws IOException {
    Encoder encoder = new DoublePrecisionEncoderV1();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    double value = 0f;
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < 2; i++) {
      Decoder decoder = new DoublePrecisionDecoderV1();
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readDouble(buffer), delta);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readDouble(buffer), delta);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readDouble(buffer), delta);
      }
    }
  }

  @Test
  public void testFloatRepeat() throws Exception {
    for (int i = 1; i <= 10; i++) {
      testFloatLength(floatList, false, i);
    }
  }

  @Test
  public void testDoubleRepeat() throws Exception {
    for (int i = 1; i <= 10; i++) {
      testDoubleLength(doubleList, false, i);
    }
  }

  @Test
  public void testFloat() throws IOException {
    Encoder encoder = new SinglePrecisionEncoderV1();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    float value = 7.101f;
    int num = 10000;
    for (int i = 0; i < num; i++) {
      encoder.encode(value + 2 * i, baos);
    }
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Decoder decoder = new SinglePrecisionDecoderV1();
    for (int i = 0; i < num; i++) {
      if (decoder.hasNext(buffer)) {
        assertEquals(value + 2 * i, decoder.readFloat(buffer), delta);
        continue;
      }
      fail();
    }
  }

  @Test
  public void testDouble() throws IOException {
    Encoder encoder = new DoublePrecisionEncoderV1();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    double value = 7.101f;
    int num = 1000;
    for (int i = 0; i < num; i++) {
      encoder.encode(value + 2 * i, baos);
    }
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Decoder decoder = new DoublePrecisionDecoderV1();
    for (int i = 0; i < num; i++) {
      if (decoder.hasNext(buffer)) {
        // System.out.println("turn "+i);
        assertEquals(value + 2 * i, decoder.readDouble(buffer), delta);
        continue;
      }
      fail();
    }
  }

  private void testFloatLength(List<Float> valueList, boolean isDebug, int repeatCount)
      throws Exception {
    Encoder encoder = new SinglePrecisionEncoderV1();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (float value : valueList) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < repeatCount; i++) {

      Decoder decoder = new SinglePrecisionDecoderV1();
      for (float value : valueList) {
        // System.out.println("Repeat: "+i+" value: "+value);
        if (decoder.hasNext(buffer)) {
          float value_ = decoder.readFloat(buffer);
          if (isDebug) {
            logger.debug("{} // {}", value_, value);
          }
          assertEquals(value, value_, delta);
          continue;
        }
        fail();
      }
    }
  }

  private void testDoubleLength(List<Double> valueList, boolean isDebug, int repeatCount)
      throws Exception {
    Encoder encoder = new DoublePrecisionEncoderV1();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (double value : valueList) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new DoublePrecisionDecoderV1();
      for (double value : valueList) {
        if (decoder.hasNext(buffer)) {
          double value_ = decoder.readDouble(buffer);
          if (isDebug) {
            logger.debug("{} // {}", value_, value);
          }
          assertEquals(value, value_, delta);
          continue;
        }
        fail();
      }
    }
  }
}
