package org.apache.iotdb.tsfile.common;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;

public class BatchDataTest {
  @Test
  public void batchInsertBooleansInSingleArrayTest() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    long[] times = new long[step1];
    boolean[] values = new boolean[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
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

    // Insert columns to do expansion on single column
    int step2 = 10;
    times = new long[step2];
    values = new boolean[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
    }
    batchData.putBooleans(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(20, expected);
  }

  @Test
  public void batchInsertBooleansSpilledTest1() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int count = 1000;
    long[] times = new long[count];
    boolean[] values = new boolean[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
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

    // Equal to realThreshold(not spilled)
    count = 1024 - 1000;
    times = new long[count];
    values = new boolean[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
    }
    batchData.putBooleans(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    count = 10;
    times = new long[count];
    values = new boolean[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
    }
    batchData.putBooleans(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1034, expected);
  }


  @Test
  public void batchInsertBooleansSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int count = 800;
    long[] times = new long[count];
    boolean[] values = new boolean[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
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

    // More than realThreshold
    count = 1200 - 800;
    times = new long[count];
    values = new boolean[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
    }
    batchData.putBooleans(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1200, expected);
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

    Assert.assertEquals(1500, expected);
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

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertBooleansInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    long[] times = new long[step1];
    boolean[] values = new boolean[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
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

    Assert.assertEquals(1500, expected);

    int step2 = 1500;
    times = new long[step2];
    values = new boolean[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
    }
    batchData.putBooleans(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertBooleansInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    long[] times = new long[step1];
    boolean[] values = new boolean[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
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

    Assert.assertEquals(2500, expected);

    int step2 = 2500;
    times = new long[step2];
    values = new boolean[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = (counter & 1) == 0;
      counter++;
    }
    batchData.putBooleans(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = (boolean) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals((expected & 1) == 0, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(5000, expected);
  }

  @Test
  public void batchInsertIntsInSingleArrayTest() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    long[] times = new long[step1];
    int[] values = new int[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // Insert columns to do expansion on single column
    int step2 = 10;
    times = new long[step2];
    values = new int[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putInts(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(20, expected);
  }

  @Test
  public void batchInsertIntsSpilledTest1() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int count = 1000;
    long[] times = new long[count];
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // Equal to realThreshold(not spilled)
    count = 1024 - 1000;
    times = new long[count];
    values = new int[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putInts(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    count = 10;
    times = new long[count];
    values = new int[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putInts(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1034, expected);
  }


  @Test
  public void batchInsertIntsSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int count = 800;
    long[] times = new long[count];
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // More than realThreshold
    count = 1200 - 800;
    times = new long[count];
    values = new int[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putInts(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1200, expected);
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

    Assert.assertEquals(1500, expected);
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

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertIntsInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    long[] times = new long[step1];
    int[] values = new int[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    Assert.assertEquals(1500, expected);

    int step2 = 1500;
    times = new long[step2];
    values = new int[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putInts(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertIntsInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT32);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    long[] times = new long[step1];
    int[] values = new int[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    Assert.assertEquals(2500, expected);

    int step2 = 2500;
    times = new long[step2];
    values = new int[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putInts(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = (int) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(5000, expected);
  }

  @Test
  public void batchInsertLongsInSingleArrayTest() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    long[] times = new long[step1];
    long[] values = new long[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // Insert columns to do expansion on single column
    int step2 = 10;
    times = new long[step2];
    values = new long[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putLongs(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(20, expected);
  }

  @Test
  public void batchInsertLongsSpilledTest1() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int count = 1000;
    long[] times = new long[count];
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // Equal to realThreshold(not spilled)
    count = 1024 - 1000;
    times = new long[count];
    values = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putLongs(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    count = 10;
    times = new long[count];
    values = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putLongs(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1034, expected);
  }


  @Test
  public void batchInsertLongsSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int count = 800;
    long[] times = new long[count];
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // More than realThreshold
    count = 1200 - 800;
    times = new long[count];
    values = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putLongs(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1200, expected);
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

    Assert.assertEquals(1500, expected);
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

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertLongsInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    long[] times = new long[step1];
    long[] values = new long[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    Assert.assertEquals(1500, expected);

    int step2 = 1500;
    times = new long[step2];
    values = new long[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putLongs(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertLongsInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.INT64);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    long[] times = new long[step1];
    long[] values = new long[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    Assert.assertEquals(2500, expected);

    int step2 = 2500;
    times = new long[step2];
    values = new long[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putLongs(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = (long) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(5000, expected);
  }

  @Test
  public void batchInsertFloatsInSingleArrayTest() {
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
  public void batchInsertFloatsSpilledTest1() {
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
  public void batchInsertFloatsSpilledTest2() {
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

    Assert.assertEquals(1500, expected);
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

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertFloatsInMultipleArraysTest2() {
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
  public void batchInsertFloatsInMultipleArraysTest3() {
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
  @Test
  public void batchInsertDoublesInSingleArrayTest() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    long[] times = new long[step1];
    double[] values = new double[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // Insert columns to do expansion on single column
    int step2 = 10;
    times = new long[step2];
    values = new double[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putDoubles(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // Prevent early stopping
    Assert.assertEquals(20, expected);
  }

  @Test
  public void batchInsertDoublesSpilledTest1() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int count = 1000;
    long[] times = new long[count];
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // Equal to realThreshold(not spilled)
    count = 1024 - 1000;
    times = new long[count];
    values = new double[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putDoubles(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    // More than realThreshold(spilled)
    count = 10;
    times = new long[count];
    values = new double[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putDoubles(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1034, expected);
  }


  @Test
  public void batchInsertDoublesSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int count = 800;
    long[] times = new long[count];
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    // More than realThreshold
    count = 1200 - 800;
    times = new long[count];
    values = new double[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putDoubles(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1200, expected);
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

    Assert.assertEquals(1500, expected);
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

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertDoublesInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    long[] times = new long[step1];
    double[] values = new double[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    Assert.assertEquals(1500, expected);

    int step2 = 1500;
    times = new long[step2];
    values = new double[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putDoubles(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertDoublesInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    long[] times = new long[step1];
    double[] values = new double[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
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

    Assert.assertEquals(2500, expected);

    int step2 = 2500;
    times = new long[step2];
    values = new double[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = counter;
      counter++;
    }
    batchData.putDoubles(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = (double) iterator.currentValue();

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expected, value, 1E-5);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(5000, expected);
  }

  @Test
  public void batchInsertBinariesInSingleArrayTest() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Insert small columns first
    int step1 = 10;
    long[] times = new long[step1];
    Binary[] values = new Binary[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
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

    // Insert columns to do expansion on single column
    int step2 = 10;
    times = new long[step2];
    values = new Binary[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
    }
    batchData.putBinaries(times, values);

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
    Assert.assertEquals(20, expected);
  }

  @Test
  public void batchInsertBinariesSpilledTest1() {
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold(=1024)
    int count = 1000;
    long[] times = new long[count];
    Binary[] values = new Binary[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
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

    // Equal to realThreshold(not spilled)
    count = 1024 - 1000;
    times = new long[count];
    values = new Binary[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
    }
    batchData.putBinaries(times, values);

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
    count = 10;
    times = new long[count];
    values = new Binary[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
    }
    batchData.putBinaries(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1034, expected);
  }


  @Test
  public void batchInsertBinariesSpilledTest2() {
    // Test without 'equal to realThreshold' insertion
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    // Less than realThreshold
    int count = 800;
    long[] times = new long[count];
    Binary[] values = new Binary[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
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

    // More than realThreshold
    count = 1200 - 800;
    times = new long[count];
    values = new Binary[count];
    for (int i = 0; i < count; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
    }
    batchData.putBinaries(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(1200, expected);
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

    Assert.assertEquals(1500, expected);
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

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertBinariesInMultipleArraysTest2() {
    // Insert huge columns with some element remained
    // Here we only iterate while loop once
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 1500;
    long[] times = new long[step1];
    Binary[] values = new Binary[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
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

    Assert.assertEquals(1500, expected);

    int step2 = 1500;
    times = new long[step2];
    values = new Binary[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
    }
    batchData.putBinaries(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(3000, expected);
  }

  @Test
  public void batchInsertBinariesInMultipleArraysTest3() {
    // Insert huge columns with some element remained
    // This test will repeat while loop multiple times
    int counter = 0;
    BatchData batchData = new BatchData(TSDataType.TEXT);
    IBatchDataIterator iterator = batchData.getBatchDataIterator();

    int step1 = 2500;
    long[] times = new long[step1];
    Binary[] values = new Binary[step1];
    for (int i = 0; i < step1; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
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

    Assert.assertEquals(2500, expected);

    int step2 = 2500;
    times = new long[step2];
    values = new Binary[step2];
    for (int i = 0; i < step2; i++) {
      times[i] = counter;
      values[i] = new Binary(String.valueOf(counter), TSFileConfig.STRING_CHARSET);
      counter++;
    }
    batchData.putBinaries(times, values);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = (Binary) iterator.currentValue();
      Binary expectedBinary = new Binary(String.valueOf(expected), TSFileConfig.STRING_CHARSET);

      Assert.assertEquals(expected, time);
      Assert.assertEquals(expectedBinary, value);
      expected++;

      iterator.next();
    }

    Assert.assertEquals(5000, expected);
  }
}
