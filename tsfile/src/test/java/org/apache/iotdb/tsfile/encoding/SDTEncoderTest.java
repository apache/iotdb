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

import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SDTEncoderTest {

  @Test
  public void testIntSingleValue() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      int value = (int) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeInt(time, value)) {
        count++;
      }
    }
    assertEquals(22, count);
  }

  @Test
  public void testDoubleSingleValue() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      double value = 10 * Math.sin(degree++ * 3.141592653589793D / 180.0D);
      if (encoder.encodeDouble(time, value)) {
        count++;
      }
    }
    assertEquals(14, count);
  }

  @Test
  public void testLongSingleValue() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      long value = (long) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeLong(time, value)) {
        count++;
      }
    }
    assertEquals(22, count);
  }

  @Test
  public void testFloatSingleValue() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      float value = (float) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeFloat(time, value)) {
        count++;
      }
    }
    assertEquals(14, count);
  }

  @Test
  public void testIntValueArray() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    int[] values = new int[100];

    for (int time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      int value = (int) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    int size = encoder.encode(timestamps, values, timestamps.length);

    assertEquals(22, size);
  }

  @Test
  public void testDoubleValueArray() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    double[] values = new double[100];

    for (int time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      double value = (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    int size = encoder.encode(timestamps, values, timestamps.length);

    assertEquals(14, size);
  }

  @Test
  public void testLongValueArray() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    long[] values = new long[100];

    for (int time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      long value = (long) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    int size = encoder.encode(timestamps, values, timestamps.length);

    assertEquals(22, size);
  }

  @Test
  public void testFloatValueArray() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    float[] values = new float[100];

    for (int time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      float value = (float) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    int size = encoder.encode(timestamps, values, timestamps.length);

    assertEquals(14, size);
  }
}
