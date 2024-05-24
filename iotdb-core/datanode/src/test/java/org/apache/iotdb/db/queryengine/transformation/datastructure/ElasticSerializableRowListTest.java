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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.datastructure.iterator.RowListForwardIterator;
import org.apache.iotdb.db.queryengine.transformation.datastructure.row.ElasticSerializableRowList;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticSerializableRowListTest extends SerializableListTest {

  private static final TSDataType[] DATA_TYPES = {
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.BOOLEAN,
    TSDataType.TEXT,
    TSDataType.TEXT
  };

  private ElasticSerializableRowList rowList;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testPutAndGet() {
    initESRowRecordList();

    testPuts();

    testGetByIndex();
  }

  private void initESRowRecordList() {
    try {
      rowList =
          new ElasticSerializableRowList(
              DATA_TYPES, QUERY_ID, MEMORY_USAGE_LIMIT_IN_MB, CACHE_SIZE);
    } catch (QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(0, rowList.size());
  }

  private void testPuts() {
    try {
      Column[] columns = generateColumns();
      rowList.put(columns);
    } catch (IOException | QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, rowList.size());
  }

  private Column[] generateColumns() {
    Column[] columns = new Column[DATA_TYPES.length + 1];

    boolean[] isNulls = new boolean[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; i++) {
      isNulls[i] = i % 7 == 0;
    }
    // Int columns
    IntColumnBuilder intColumnBuilder = new IntColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; i++) {
      if (i % 7 == 0) {
        intColumnBuilder.appendNull();
      } else {
        intColumnBuilder.writeInt(i);
      }
    }
    columns[0] = intColumnBuilder.build();

    // Long columns
    LongColumnBuilder longColumnBuilder = new LongColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; i++) {
      if (i % 7 == 0) {
        longColumnBuilder.appendNull();
      } else {
        longColumnBuilder.writeLong(i);
      }
    }
    columns[1] = longColumnBuilder.build();

    // Float columns
    FloatColumnBuilder floatColumnBuilder = new FloatColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; i++) {
      if (i % 7 == 0) {
        floatColumnBuilder.appendNull();
      } else {
        floatColumnBuilder.writeFloat(i);
      }
    }
    columns[2] = floatColumnBuilder.build();

    // Double columns
    DoubleColumnBuilder doubleColumnBuilder = new DoubleColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; i++) {
      if (i % 7 == 0) {
        doubleColumnBuilder.appendNull();
      } else {
        doubleColumnBuilder.writeDouble(i);
      }
    }
    columns[3] = doubleColumnBuilder.build();

    // Boolean columns
    BooleanColumnBuilder booleanColumnBuilder = new BooleanColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; i++) {
      if (i % 7 == 0) {
        booleanColumnBuilder.appendNull();
      } else {
        booleanColumnBuilder.writeBoolean(i % 2 == 0);
      }
    }
    columns[4] = booleanColumnBuilder.build();

    // Binary columns
    BinaryColumnBuilder binaryColumnBuilder = new BinaryColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; i++) {
      if (i % 7 == 0) {
        binaryColumnBuilder.appendNull();
      } else {
        Binary binary = BytesUtils.valueOf(String.valueOf(i));
        binaryColumnBuilder.writeBinary(binary);
      }
    }
    columns[5] = binaryColumnBuilder.build();

    // Another binary columns
    Binary[] binaries = columns[5].getBinaries().clone();
    columns[6] = new BinaryColumn(ITERATION_TIMES, Optional.of(isNulls), binaries.clone());

    // The last time columns
    TimeColumnBuilder timeColumnBuilder = new TimeColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < ITERATION_TIMES; i++) {
      timeColumnBuilder.writeLong(i);
    }
    columns[7] = timeColumnBuilder.build();

    return columns;
  }

  private void testRowRecord(Object[] rowRecord, int expected) {
    for (int j = 0; j < DATA_TYPES.length; ++j) {
      switch (DATA_TYPES[j]) {
        case INT32:
          assertEquals(expected, (int) rowRecord[j]);
          break;
        case INT64:
          assertEquals(expected, (long) rowRecord[j]);
          break;
        case FLOAT:
          assertEquals(expected, (float) rowRecord[j], 0);
          break;
        case DOUBLE:
          assertEquals(expected, (double) rowRecord[j], 0);
          break;
        case BOOLEAN:
          assertEquals(expected % 2 == 0, rowRecord[j]);
          break;
        case TEXT:
          assertEquals(BytesUtils.valueOf(String.valueOf(expected)), rowRecord[j]);
          break;
      }
    }
    assertEquals(DATA_TYPES.length, rowRecord.length - 1);
  }

  private void testGetByIndex() {
    try {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i % 7 == 0) {
          assertTrue(rowList.fieldsHasAnyNull(i));
        } else {
          assertFalse(rowList.fieldsHasAnyNull(i));
          testRowRecord(rowList.getRowRecord(i), i);
        }
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testMemoryControl() {
    initESRowRecordList();

    int byteLengthMin = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 2;
    int byteLengthMax = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 8;

    try {
      Column[] columns =
          generateColumnsWithRandomBinaries(ITERATION_TIMES, byteLengthMin, byteLengthMax);
      rowList.put(columns);
      rowList.setEvictionUpperBound(rowList.size());
      RowListForwardIterator iterator = rowList.constructIterator();
      testRowList(iterator);

      byteLengthMin = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 16;
      byteLengthMax = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 32;
      columns = generateColumnsWithRandomBinaries(ITERATION_TIMES, byteLengthMin, byteLengthMax);
      rowList.put(columns);
      rowList.setEvictionUpperBound(rowList.size());
      testRowList(iterator);

      byteLengthMin = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 256;
      byteLengthMax = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 512;
      columns = generateColumnsWithRandomBinaries(ITERATION_TIMES, byteLengthMin, byteLengthMax);
      rowList.put(columns);
      rowList.setEvictionUpperBound(rowList.size());
      testRowList(iterator);

      columns = generateColumnsWithRandomBinaries(ITERATION_TIMES, byteLengthMin, byteLengthMax);
      rowList.put(columns);
      rowList.setEvictionUpperBound(rowList.size());
      testRowList(iterator);

      columns = generateColumnsWithRandomBinaries(ITERATION_TIMES, byteLengthMin, byteLengthMax);
      rowList.put(columns);
      rowList.setEvictionUpperBound(rowList.size());
      testRowList(iterator);

      assertEquals(ITERATION_TIMES * 5, rowList.size());
    } catch (QueryProcessException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void testRowList(RowListForwardIterator iterator) throws IOException {
    int index = 0;

    while (iterator.hasNext()) {
      iterator.next();
      Column[] columns = iterator.currentBlock();
      int count = columns[0].getPositionCount();
      for (int i = 0; i < count; i++, index++) {
        if (index % 7 == 0) {
          assertTrue(fieldHasAnyNull(columns, i));
        } else {
          assertFalse(fieldHasAnyNull(columns, i));
        }
      }
    }
  }

  private boolean fieldHasAnyNull(Column[] columns, int index) {
    for (Column column : columns) {
      if (column.isNull(index)) {
        return true;
      }
    }
    return false;
  }

  private Column[] generateColumnsWithRandomBinaries(
      int iterTimes, int byteLengthMin, int byteLengthMax) {
    Random random = new Random();
    Column[] columns = new Column[DATA_TYPES.length + 1];

    // Int columns
    IntColumnBuilder intColumnBuilder = new IntColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < iterTimes; i++) {
      if (i % 7 == 0) {
        intColumnBuilder.appendNull();
      } else {
        intColumnBuilder.writeInt(i);
      }
    }
    columns[0] = intColumnBuilder.build();

    // Long columns
    LongColumnBuilder longColumnBuilder = new LongColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < iterTimes; i++) {
      if (i % 7 == 0) {
        longColumnBuilder.appendNull();
      } else {
        longColumnBuilder.writeLong(i);
      }
    }
    columns[1] = longColumnBuilder.build();

    // Float columns
    FloatColumnBuilder floatColumnBuilder = new FloatColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < iterTimes; i++) {
      if (i % 7 == 0) {
        floatColumnBuilder.appendNull();
      } else {
        floatColumnBuilder.writeFloat(i);
      }
    }
    columns[2] = floatColumnBuilder.build();

    // Double columns
    DoubleColumnBuilder doubleColumnBuilder = new DoubleColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < iterTimes; i++) {
      if (i % 7 == 0) {
        doubleColumnBuilder.appendNull();
      } else {
        doubleColumnBuilder.writeDouble(i);
      }
    }
    columns[3] = doubleColumnBuilder.build();

    // Boolean columns
    BooleanColumnBuilder booleanColumnBuilder = new BooleanColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < iterTimes; i++) {
      if (i % 7 == 0) {
        booleanColumnBuilder.appendNull();
      } else {
        booleanColumnBuilder.writeBoolean(i % 2 == 0);
      }
    }
    columns[4] = booleanColumnBuilder.build();

    // Binary columns
    BinaryColumnBuilder binaryColumnBuilder = new BinaryColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < iterTimes; i++) {
      if (i % 7 == 0) {
        binaryColumnBuilder.appendNull();
      } else {
        Binary binary =
            BytesUtils.valueOf(
                generateStringByLength(
                    byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin)));
        binaryColumnBuilder.writeBinary(binary);
      }
    }
    columns[5] = binaryColumnBuilder.build();

    // Another binary columns
    BinaryColumnBuilder anotherbinaryColumnBuilder = new BinaryColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < iterTimes; i++) {
      if (i % 7 == 0) {
        anotherbinaryColumnBuilder.appendNull();
      } else {
        Binary binary =
            BytesUtils.valueOf(
                generateStringByLength(
                    byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin)));
        anotherbinaryColumnBuilder.writeBinary(binary);
      }
    }
    columns[6] = anotherbinaryColumnBuilder.build();

    // The last time columns
    TimeColumnBuilder timeColumnBuilder = new TimeColumnBuilder(null, ITERATION_TIMES);
    for (int i = 0; i < iterTimes; i++) {
      timeColumnBuilder.writeLong(i);
    }
    columns[7] = timeColumnBuilder.build();

    return columns;
  }

  private String generateStringByLength(int length) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < length; ++i) {
      stringBuilder.append('.');
    }
    return stringBuilder.toString();
  }
}
