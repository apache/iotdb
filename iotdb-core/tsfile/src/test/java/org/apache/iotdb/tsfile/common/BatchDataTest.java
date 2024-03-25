package org.apache.iotdb.tsfile.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.junit.Assert;
import org.junit.Test;

public class BatchDataTest {
  @Test
  public void batchInsertInSingleArrayTest() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    long[] times = new long[step1];
    float[] values = new float[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // Insert columns to do expansion on single column
    int step2 = 10;
    times = new long[step2];
    values = new float[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putFloats(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(20, expected);
  }

  @Test
  public void batchInsertSpilledTest1() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int count = 1000;
    long[] times = new long[count];
    float[] values = new float[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // Equal to realThreshold(not spilled)
    count = 1024 - 1000;
    times = new long[count];
    values = new float[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putFloats(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    count = 10;
    times = new long[count];
    values = new float[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putFloats(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1034, expected);
  }


  @Test
  public void batchInsertSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int count = 800;
    long[] times = new long[count];
    float[] values = new float[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // More than realThreshold
    count = 1200 - 800;
    times = new long[count];
    values = new float[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putFloats(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1200, expected);
  }

  @Test
  public void batchInsertSpilledTest3() {
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

    Assert.assertEquals(1500, expected);
  }

  @Test
  public void batchInsertInMultipleArraysTest1() {
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

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    long[] times = new long[step1];
    float[] values = new float[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    Assert.assertEquals(1500, expected);

    int step2 = 1500;
    times = new long[step2];
    values = new float[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putFloats(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    long[] times = new long[step1];
    float[] values = new float[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    Assert.assertEquals(2500, expected);

    int step2 = 2500;
    times = new long[step2];
    values = new float[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putFloats(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = (float) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(5000, expected);
  }
}
