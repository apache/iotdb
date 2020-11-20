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

package org.apache.iotdb.tsfile.encoding;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.iotdb.tsfile.encoding.encoder.SdtEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Test;

public class SdtEncoderTest {

  @Test
  public void testIntSingleValue() throws IOException {
    SdtEncoder encoder = new SdtEncoder(TSDataType.INT32);
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      //generate data in sine wave pattern
      int value = (int) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeInt(time, value)) {
        count++;
      }
    }
    assertEquals(count, 22);
  }

  @Test
  public void testDoubleSingleValue() throws IOException {
    SdtEncoder encoder = new SdtEncoder(TSDataType.DOUBLE);
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      //generate data in sine wave pattern
      double value = 10 * Math.sin(degree++ * 3.141592653589793D / 180.0D);
      if (encoder.encodeDouble(time, value)) {
        count++;
      }
    }
    assertEquals(count, 14);
  }

  @Test
  public void testLongSingleValue() throws IOException {
    SdtEncoder encoder = new SdtEncoder(TSDataType.INT64);
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      //generate data in sine wave pattern
      long value = (long) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeLong(time, value)) {
        count++;
      }
    }
    assertEquals(count, 22);
  }

  @Test
  public void testFloatSingleValue() throws IOException {
    SdtEncoder encoder = new SdtEncoder(TSDataType.FLOAT);
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      //generate data in sine wave pattern
      float value = (float) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeFloat(time, value)) {
        count++;
      }
    }
    assertEquals(count, 14);
  }

  @Test
  public void testIntValueArray() {
  SdtEncoder encoder = new SdtEncoder(TSDataType.INT32);
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    int[] values = new int[100];

    for (int time = 0; time < 100; time++) {
      //generate data in sine wave pattern
      int value = (int) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    encoder.encode(timestamps, values);

    assertEquals(encoder.getIntValues().length, 22);
  }

  @Test
  public void testDoubleValueArray() throws IOException {
    SdtEncoder encoder = new SdtEncoder(TSDataType.DOUBLE);
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    double[] values = new double[100];

    for (int time = 0; time < 100; time++) {
      //generate data in sine wave pattern
      double value = (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    encoder.encode(timestamps, values);

    assertEquals(encoder.getDoubleValues().length, 14);
  }

  @Test
  public void testLongValueArray() {
    SdtEncoder encoder = new SdtEncoder(TSDataType.INT64);
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    long[] values = new long[100];

    for (int time = 0; time < 100; time++) {
      //generate data in sine wave pattern
      long value = (long) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    encoder.encode(timestamps, values);

    assertEquals(encoder.getLongValues().length, 22);
  }

  @Test
  public void testFloatValueArray() {
    SdtEncoder encoder = new SdtEncoder(TSDataType.FLOAT);
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    float[] values = new float[100];

    for (int time = 0; time < 100; time++) {
      //generate data in sine wave pattern
      float value = (float) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    encoder.encode(timestamps, values);

    assertEquals(encoder.getFloatValues().length, 14);
  }
}
