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

package org.apache.iotdb.db.queryengine.transformation.datastructure;

import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.SerializableTVList;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SerializableTVListTest extends SerializableListTest {
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void serializableBooleanTVListTest() {
    List<Boolean> compared = new ArrayList<>();
    SerializableTVList target = SerializableTVList.construct(QUERY_ID);

    serializeAndDeserializeBooleanTVListTest(compared, target);
  }

  @Test
  public void serializableIntTVListTest() {
    List<Integer> compared = new ArrayList<>();
    SerializableTVList target = SerializableTVList.construct(QUERY_ID);

    serializeAndDeserializeIntTVListTest(compared, target);
  }

  @Test
  public void serializableLongTVListTest() {
    List<Long> compared = new ArrayList<>();
    SerializableTVList target = SerializableTVList.construct(QUERY_ID);

    serializeAndDeserializeLongTVListTest(compared, target);
  }

  @Test
  public void serializableFloatTVListTest() {
    List<Float> compared = new ArrayList<>();
    SerializableTVList target = SerializableTVList.construct(QUERY_ID);

    serializeAndDeserializeFloatTVListTest(compared, target);
  }

  @Test
  public void serializableDoubleTVListTest() {
    List<Double> compared = new ArrayList<>();
    SerializableTVList target = SerializableTVList.construct(QUERY_ID);

    serializeAndDeserializeDoubleTVListTest(compared, target);
  }

  @Test
  public void serializableBinaryTVListTest() {
    List<Binary> compared = new ArrayList<>();
    SerializableTVList target = SerializableTVList.construct(QUERY_ID);

    serializeAndDeserializeBinaryTVListTest(compared, target);
  }

  private void serializeAndDeserializeBooleanTVListTest(
      List<Boolean> compared, SerializableTVList target) {
    generateBooleanData(compared, target);

    serializeAndDeserializeBooleanListOnce(compared, target);
    serializeAndDeserializeBooleanListOnce(compared, target);
  }

  private static void generateBooleanData(List<Boolean> compared, SerializableTVList target) {
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, ITERATION_TIMES);
    ColumnBuilder valueBuilder = new BooleanColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      compared.add(i % 2 == 0);

      timeBuilder.writeLong(i);
      valueBuilder.writeBoolean(i % 2 == 0);
    }
    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    target.putColumns((TimeColumn) times, values);
  }

  protected void serializeAndDeserializeBooleanListOnce(
      List<Boolean> compared, SerializableTVList target) {
    try {
      target.serialize();
    } catch (IOException e) {
      fail();
    }
    try {
      target.deserialize();
    } catch (IOException e) {
      fail();
    }
    checkBooleanListEquality(compared, target);
  }

  private static void checkBooleanListEquality(List<Boolean> compared, SerializableTVList target) {
    int count = 0;
    ForwardIterator iterator = new ForwardIterator(target);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      boolean value = iterator.currentBoolean();

      assertEquals(count, time);
      assertEquals(compared.get(count), value);

      iterator.next();
      count++;
    }
    assertEquals(ITERATION_TIMES, count);
  }

  private void serializeAndDeserializeIntTVListTest(
      List<Integer> compared, SerializableTVList target) {
    generateIntData(compared, target);

    serializeAndDeserializeIntListOnce(compared, target);
    serializeAndDeserializeIntListOnce(compared, target);
  }

  private static void generateIntData(List<Integer> compared, SerializableTVList target) {
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, ITERATION_TIMES);
    ColumnBuilder valueBuilder = new IntColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      compared.add(i);

      timeBuilder.writeLong(i);
      valueBuilder.writeInt(i);
    }
    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    target.putColumns((TimeColumn) times, values);
  }

  protected void serializeAndDeserializeIntListOnce(
      List<Integer> compared, SerializableTVList target) {
    try {
      target.serialize();
    } catch (IOException e) {
      fail();
    }
    try {
      target.deserialize();
    } catch (IOException e) {
      fail();
    }
    checkIntListEquality(compared, target);
  }

  private static void checkIntListEquality(List<Integer> compared, SerializableTVList target) {
    int count = 0;
    ForwardIterator iterator = new ForwardIterator(target);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      int value = iterator.currentInt();

      assertEquals(count, time);
      assertEquals((int) compared.get(count), value);

      iterator.next();
      count++;
    }
    assertEquals(ITERATION_TIMES, count);
  }

  private void serializeAndDeserializeLongTVListTest(
      List<Long> compared, SerializableTVList target) {
    generateLongData(compared, target);

    serializeAndDeserializeLongListOnce(compared, target);
    serializeAndDeserializeLongListOnce(compared, target);
  }

  private static void generateLongData(List<Long> compared, SerializableTVList target) {
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, ITERATION_TIMES);
    ColumnBuilder valueBuilder = new LongColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      compared.add((long) i);

      timeBuilder.writeLong(i);
      valueBuilder.writeLong(i);
    }
    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    target.putColumns((TimeColumn) times, values);
  }

  protected void serializeAndDeserializeLongListOnce(
      List<Long> compared, SerializableTVList target) {
    try {
      target.serialize();
    } catch (IOException e) {
      fail();
    }
    try {
      target.deserialize();
    } catch (IOException e) {
      fail();
    }
    checkLongListEquality(compared, target);
  }

  private static void checkLongListEquality(List<Long> compared, SerializableTVList target) {
    int count = 0;
    ForwardIterator iterator = new ForwardIterator(target);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      long value = iterator.currentLong();

      assertEquals(count, time);
      assertEquals((long) compared.get(count), value);

      iterator.next();
      count++;
    }
    assertEquals(ITERATION_TIMES, count);
  }

  private void serializeAndDeserializeFloatTVListTest(
      List<Float> compared, SerializableTVList target) {
    generateFloatData(compared, target);

    serializeAndDeserializeFloatListOnce(compared, target);
    serializeAndDeserializeFloatListOnce(compared, target);
  }

  private static void generateFloatData(List<Float> compared, SerializableTVList target) {
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, ITERATION_TIMES);
    ColumnBuilder valueBuilder = new FloatColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      compared.add((float) i);

      timeBuilder.writeLong(i);
      valueBuilder.writeFloat((float) i);
    }
    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    target.putColumns((TimeColumn) times, values);
  }

  protected void serializeAndDeserializeFloatListOnce(
      List<Float> compared, SerializableTVList target) {
    try {
      target.serialize();
    } catch (IOException e) {
      fail();
    }
    try {
      target.deserialize();
    } catch (IOException e) {
      fail();
    }
    checkFloatListEquality(compared, target);
  }

  private static void checkFloatListEquality(List<Float> compared, SerializableTVList target) {
    int count = 0;
    ForwardIterator iterator = new ForwardIterator(target);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      float value = iterator.currentFloat();

      assertEquals(count, time);
      assertEquals(compared.get(count), value, 0);

      iterator.next();
      count++;
    }
    assertEquals(ITERATION_TIMES, count);
  }

  private void serializeAndDeserializeDoubleTVListTest(
      List<Double> compared, SerializableTVList target) {
    generateDoubleData(compared, target);

    serializeAndDeserializeDoubleListOnce(compared, target);
    serializeAndDeserializeDoubleListOnce(compared, target);
  }

  private static void generateDoubleData(List<Double> compared, SerializableTVList target) {
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, ITERATION_TIMES);
    ColumnBuilder valueBuilder = new DoubleColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      compared.add((double) i);

      timeBuilder.writeLong(i);
      valueBuilder.writeDouble(i);
    }
    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    target.putColumns((TimeColumn) times, values);
  }

  protected void serializeAndDeserializeDoubleListOnce(
      List<Double> compared, SerializableTVList target) {
    try {
      target.serialize();
    } catch (IOException e) {
      fail();
    }
    try {
      target.deserialize();
    } catch (IOException e) {
      fail();
    }
    checkDoubleListEquality(compared, target);
  }

  private static void checkDoubleListEquality(List<Double> compared, SerializableTVList target) {
    int count = 0;
    ForwardIterator iterator = new ForwardIterator(target);

    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      double value = iterator.currentDouble();

      assertEquals(count, time);
      assertEquals(compared.get(count), value, 0);

      iterator.next();
      count++;
    }
    assertEquals(ITERATION_TIMES, count);
  }

  private void serializeAndDeserializeBinaryTVListTest(
      List<Binary> compared, SerializableTVList target) {
    generateBinaryData(compared, target);

    serializeAndDeserializeBinaryListOnce(compared, target);
    serializeAndDeserializeBinaryListOnce(compared, target);
  }

  private static void generateBinaryData(List<Binary> compared, SerializableTVList target) {
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, ITERATION_TIMES);
    ColumnBuilder valueBuilder = new BinaryColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      Binary value = BytesUtils.valueOf(String.valueOf(i));
      compared.add(value);

      timeBuilder.writeLong(i);
      valueBuilder.writeBinary(value);
    }
    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    target.putColumns((TimeColumn) times, values);
  }

  protected void serializeAndDeserializeBinaryListOnce(
      List<Binary> compared, SerializableTVList target) {
    try {
      target.serialize();
    } catch (IOException e) {
      fail();
    }
    try {
      target.deserialize();
    } catch (IOException e) {
      fail();
    }
    checkBinaryListEquality(compared, target);
  }

  private static void checkBinaryListEquality(List<Binary> compared, SerializableTVList target) {
    int count = 0;
    ForwardIterator iterator = new ForwardIterator(target);
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      Binary value = iterator.currentBinary();

      assertEquals(count, time);
      assertEquals(compared.get(count), value);

      iterator.next();
      count++;
    }
    assertEquals(ITERATION_TIMES, count);
  }

  private static class ForwardIterator {
    int index;
    int offset;
    List<Column> timeColumns;
    List<Column> valueColumns;

    ForwardIterator(SerializableTVList tvList) {
      timeColumns = tvList.getTimeColumns();
      valueColumns = tvList.getValueColumns();
    }

    public boolean hasNext() {
      return index < timeColumns.size();
    }

    public long currentTime() {
      return timeColumns.get(index).getLong(offset);
    }

    public boolean currentBoolean() {
      return valueColumns.get(index).getBoolean(offset);
    }

    public int currentInt() {
      return valueColumns.get(index).getInt(offset);
    }

    public long currentLong() {
      return valueColumns.get(index).getLong(offset);
    }

    public float currentFloat() {
      return valueColumns.get(index).getFloat(offset);
    }

    public double currentDouble() {
      return valueColumns.get(index).getDouble(offset);
    }

    public Binary currentBinary() {
      return valueColumns.get(index).getBinary(offset);
    }

    public void next() {
      if (offset == timeColumns.get(index).getPositionCount() - 1) {
        offset = 0;
        index++;
      } else {
        offset++;
      }
    }
  }
}
