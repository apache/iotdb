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

package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BatchDataTest {

  @Test
  public void testInt() {
    BatchData batchData = new BatchData(TSDataType.INT32);
    assertTrue(batchData.isEmpty());
    int value = 0;
    for (long time = 0; time < 10; time++) {
      batchData.putAnObject(time, value);
      value++;
    }
    assertEquals(TSDataType.INT32, batchData.getDataType());
    int res = 0;
    long time = 0;
    while (batchData.hasCurrent()) {
      assertEquals(time, batchData.currentTime());
      assertEquals(res, (int) batchData.currentValue());
      assertEquals(res, batchData.currentTsPrimitiveType().getInt());
      batchData.next();
      res++;
      time++;
    }
    batchData.resetBatchData();

    IPointReader reader = batchData.getBatchDataIterator();
    try {
      res = 0;
      time = 0;
      while (reader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = reader.nextTimeValuePair();
        assertEquals(time, timeValuePair.getTimestamp());
        assertEquals(res, timeValuePair.getValue().getInt());
        res++;
        time++;
      }
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testSignal() {
    BatchData batchData = SignalBatchData.getInstance();
    try {
      batchData.hasCurrent();
    } catch (UnsupportedOperationException e) {
      return;
    }
    fail();
  }

  @Test
  public void batchInsertBooleansInSingleArrayTest() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    int step2 = 10;
    int total = step1 + step2;
    long[] times = new long[total];
    boolean[] values = new boolean[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = (i & 1) == 0;
    }

    batchData.putBooleans(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    // Insert columns to do expansion on single column
    batchData.putBooleans(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBooleansSpilledTest1() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int step1 = 1000;
    int step2 = 24;
    int step3 = 10;
    int total = step1 + step2 + step3;
    long[] times = new long[total];
    boolean[] values = new boolean[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = (i & 1) == 0;
    }

    batchData.putBooleans(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    // Equal to realThreshold(not spilled)
    batchData.putBooleans(times, values, step1, step1 + step2);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    batchData.putBooleans(times, values, step1 + step2, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBooleansSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int step1 = 800;
    int step2 = 400;
    int total = step1 + step2;
    long[] times = new long[total];
    boolean[] values = new boolean[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = (i & 1) == 0;
    }

    batchData.putBooleans(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold
    batchData.putBooleans(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBooleansSpilledTest3() {
    // Test insertion with `more than realThreshold` points
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // 1500 elements would cause spilled
    int count = 1500;
    long[] times = new long[count];
    boolean[] values = new boolean[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = (i & 1) == 0;
    }
    batchData.putBooleans(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertBooleansInMultipleArraysTest1() {
    // Directly insert huge columns
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int count = 3000;
    long[] times = new long[count];
    boolean[] values = new boolean[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = (i & 1) == 0;
    }
    batchData.putBooleans(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertBooleansInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    int step2 = 1500;
    int total = step1 + step2;
    long[] times = new long[total];
    boolean[] values = new boolean[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = (i & 1) == 0;
    }

    batchData.putBooleans(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putBooleans(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBooleansInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    int step2 = 2500;
    int total = step1 + step2;
    long[] times = new long[total];
    boolean[] values = new boolean[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = (i & 1) == 0;
    }

    batchData.putBooleans(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putBooleans(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertIntsInSingleArrayTest() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    int step2 = 10;
    int total = step1 + step2;
    long[] times = new long[total];
    int[] values = new int[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putInts(times, values, 0, step1);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // Insert columns to do expansion on single column
    batchData.putInts(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertIntsSpilledTest1() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int step1 = 1000;
    int step2 = 24;
    int step3 = 10;
    int total = step1 + step2 + step3;
    long[] times = new long[total];
    int[] values = new int[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putInts(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // Equal to realThreshold(not spilled)
    batchData.putInts(times, values, step1, step1 + step2);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    batchData.putInts(times, values, step1 + step2, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertIntsSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int step1 = 800;
    int step2 = 400;
    int total = step1 + step2;
    long[] times = new long[total];
    int[] values = new int[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putInts(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold
    batchData.putInts(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertIntsSpilledTest3() {
    // Test insertion with `more than realThreshold` points
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // 1500 elements would cause spilled
    int count = 1500;
    long[] times = new long[count];
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putInts(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertIntsInMultipleArraysTest1() {
    // Directly insert huge columns
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int count = 3000;
    long[] times = new long[count];
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putInts(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertIntsInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    int step2 = 1500;
    int total = step1 + step2;
    long[] times = new long[total];
    int[] values = new int[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putInts(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putInts(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertIntsInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    int step2 = 2500;
    int total = step1 + step2;
    long[] times = new long[total];
    int[] values = new int[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putInts(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putInts(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertLongsInSingleArrayTest() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    int step2 = 10;
    int total = step1 + step2;
    long[] times = new long[total];
    long[] values = new long[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putLongs(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // Insert columns to do expansion on single column
    batchData.putLongs(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertLongsSpilledTest1() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int step1 = 1000;
    int step2 = 24;
    int step3 = 10;
    int total = step1 + step2 + step3;
    long[] times = new long[total];
    long[] values = new long[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putLongs(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // Equal to realThreshold(not spilled)
    batchData.putLongs(times, values, step1, step1 + step2);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    batchData.putLongs(times, values, step1 + step2, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertLongsSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int step1 = 800;
    int step2 = 400;
    int total = step1 + step2;
    long[] times = new long[total];
    long[] values = new long[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putLongs(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold
    batchData.putLongs(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertLongsSpilledTest3() {
    // Test insertion with `more than realThreshold` points
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // 1500 elements would cause spilled
    int count = 1500;
    long[] times = new long[count];
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putLongs(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertLongsInMultipleArraysTest1() {
    // Directly insert huge columns
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int count = 3000;
    long[] times = new long[count];
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putLongs(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertLongsInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    int step2 = 1500;
    int total = step1 + step2;
    long[] times = new long[total];
    long[] values = new long[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putLongs(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putLongs(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertLongsInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    int step2 = 2500;
    int total = step1 + step2;
    long[] times = new long[total];
    long[] values = new long[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putLongs(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putLongs(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertFloatsInSingleArrayTest() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 10;
    int step2 = 10;
    int total = step1 + step2;
    long[] times = new long[total];
    float[] values = new float[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    // Insert small columns first
    batchData.putFloats(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // Insert columns to do expansion on single column
    batchData.putFloats(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertFloatsSpilledTest1() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1000;
    int step2 = 24;
    int step3 = 10;
    int total = step1 + step2 + step3;
    long[] times = new long[total];
    float[] values = new float[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    // Less than realThreshold(=1024)
    batchData.putFloats(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // Equal to realThreshold(not spilled)
    batchData.putFloats(times, values, step1, step1 + step2);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    batchData.putFloats(times, values, step1 + step2, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertFloatsSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int step1 = 800;
    int step2 = 400;
    int total = step1 + step2;
    long[] times = new long[total];
    float[] values = new float[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putFloats(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();
      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // More than realThreshold
    batchData.putFloats(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertFloatsSpilledTest3() {
    // Test insertion with `more than realThreshold` points
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // 1500 elements would cause spilled
    int count = 1500;
    long[] times = new long[count];
    float[] values = new float[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putFloats(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertFloatsInMultipleArraysTest1() {
    // Directly insert huge columns
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int count = 3000;
    long[] times = new long[count];
    float[] values = new float[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putFloats(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertFloatsInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    int step2 = 1500;
    int total = step1 + step2;
    long[] times = new long[total];
    float[] values = new float[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putFloats(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putFloats(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertFloatsInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    int step2 = 2500;
    int total = step1 + step2;
    long[] times = new long[total];
    float[] values = new float[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putFloats(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putFloats(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertDoublesInSingleArrayTest() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    int step2 = 10;
    int total = step1 + step2;
    long[] times = new long[total];
    double[] values = new double[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putDoubles(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // Insert columns to do expansion on single column
    batchData.putDoubles(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertDoublesSpilledTest1() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int step1 = 1000;
    int step2 = 24;
    int step3 = 10;
    int total = step1 + step2 + step3;
    long[] times = new long[total];
    double[] values = new double[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putDoubles(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // Equal to realThreshold(not spilled)
    batchData.putDoubles(times, values, step1, step1 + step2);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    batchData.putDoubles(times, values, step1 + step2, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertDoublesSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int step1 = 800;
    int step2 = 400;
    int total = step1 + step2;
    long[] times = new long[total];
    double[] values = new double[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putDoubles(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // More than realThreshold
    batchData.putDoubles(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertDoublesSpilledTest3() {
    // Test insertion with `more than realThreshold` points
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // 1500 elements would cause spilled
    int count = 1500;
    long[] times = new long[count];
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putDoubles(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertDoublesInMultipleArraysTest1() {
    // Directly insert huge columns
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int count = 3000;
    long[] times = new long[count];
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = i;
    }
    batchData.putDoubles(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertDoublesInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    int step2 = 1500;
    int total = step1 + step2;
    long[] times = new long[total];
    double[] values = new double[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putDoubles(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putDoubles(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertDoublesInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    int step2 = 2500;
    int total = step1 + step2;
    long[] times = new long[total];
    double[] values = new double[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = i;
    }

    batchData.putDoubles(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putDoubles(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBinariesInSingleArrayTest() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    int step2 = 10;
    int total = step1 + step2;
    long[] times = new long[total];
    Binary[] values = new Binary[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);
    }

    batchData.putBinaries(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();

      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);
      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    // Insert columns to do expansion on single column
    batchData.putBinaries(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();

      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);
      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBinariesSpilledTest1() {
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int step1 = 1000;
    int step2 = 24;
    int step3 = 10;
    int total = step1 + step2 + step3;
    long[] times = new long[total];
    Binary[] values = new Binary[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);
    }

    batchData.putBinaries(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    // Equal to realThreshold(not spilled)
    batchData.putBinaries(times, values, step1, step1 + step2);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    batchData.putBinaries(times, values, step1 + step2, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBinariesSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int step1 = 800;
    int step2 = 400;
    int total = step1 + step2;
    long[] times = new long[total];
    Binary[] values = new Binary[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);
    }

    batchData.putBinaries(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold
    batchData.putBinaries(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBinariesSpilledTest3() {
    // Test insertion with `more than realThreshold` points
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // 1500 elements would cause spilled
    int count = 1500;
    long[] times = new long[count];
    Binary[] values = new Binary[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);
    }
    batchData.putBinaries(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertBinariesInMultipleArraysTest1() {
    // Directly insert huge columns
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int count = 3000;
    long[] times = new long[count];
    Binary[] values = new Binary[count];
    for (int i = 0; i < count; i++) {
      times[i] = i;
      values[i] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);
    }
    batchData.putBinaries(times, values);

    int expected = 0;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(count, expected);
  }

  @Test
  public void batchInsertBinariesInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    int step2 = 1500;
    int total = step1 + step2;
    long[] times = new long[total];
    Binary[] values = new Binary[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);
    }

    batchData.putBinaries(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putBinaries(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }

  @Test
  public void batchInsertBinariesInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int expected = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    int step2 = 2500;
    int total = step1 + step2;
    long[] times = new long[total];
    Binary[] values = new Binary[total];
    for (int i = 0; i < total; i++) {
      times[i] = i;
      values[i] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);
    }

    batchData.putBinaries(times, values, 0, step1);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(step1, expected);

    batchData.putBinaries(times, values, step1, total);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }
    Assert.assertEquals(total, expected);
  }
}
