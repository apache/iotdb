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
import org.apache.iotdb.db.queryengine.transformation.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.*;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;

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

public class ElasticSerializableRowRecordListTest extends SerializableListTest {

  private static final TSDataType[] DATA_TYPES = {
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.BOOLEAN,
    TSDataType.TEXT,
    TSDataType.TEXT
  };

  private ElasticSerializableRowRecordList rowRecordList;

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
  public void testESRowRecordList() {
    initESRowRecordList();

    testPut();

    testOrderedAccessByIndex();
  }

  @Test
  public void testInsertColumns() {
    initESRowRecordList();

    testPuts();

    testOrderedAccessByIndex();
  }

  private void initESRowRecordList() {
    try {
      rowRecordList =
          new ElasticSerializableRowRecordList(
              DATA_TYPES, QUERY_ID, MEMORY_USAGE_LIMIT_IN_MB, CACHE_SIZE);
    } catch (QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(0, rowRecordList.size());
  }

  private void testPut() {
    try {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        Object[] rowRecord = new Object[DATA_TYPES.length + 1];
        rowRecord[DATA_TYPES.length] = (long) i;
        if (i % 7 != 0) {
          for (int j = 0; j < DATA_TYPES.length; ++j) {
            switch (DATA_TYPES[j]) {
              case INT32:
                rowRecord[j] = i;
                break;
              case INT64:
                rowRecord[j] = (long) i;
                break;
              case FLOAT:
                rowRecord[j] = (float) i;
                break;
              case DOUBLE:
                rowRecord[j] = (double) i;
                break;
              case BOOLEAN:
                rowRecord[j] = i % 2 == 0;
                break;
              case TEXT:
                rowRecord[j] = BytesUtils.valueOf(String.valueOf(i));
                break;
            }
          }
        }

        rowRecordList.put(rowRecord);
      }
    } catch (IOException | QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, rowRecordList.size());
  }

  private void testPuts() {
    try {
      Column[] columns = generateColumns();
      rowRecordList.put(columns);
    } catch (IOException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, rowRecordList.size());
  }

  private Column[] generateColumns() {
    Column[] columns = new Column[DATA_TYPES.length + 1];

    boolean[] isNulls = new boolean[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; i++) {
      isNulls[i] = i % 7 == 0;
    }
    // Int columns
    int[] ints = new int[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      ints[i] = i;
    }
    columns[0] = new IntColumn(ITERATION_TIMES, Optional.of(isNulls), ints);

    // Long columns
    long[] longs = new long[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      longs[i] = i;
    }
    columns[1] = new LongColumn(ITERATION_TIMES, Optional.of(isNulls), longs);

    // Float columns
    float[] floats = new float[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      floats[i] = i;
    }
    columns[2] = new FloatColumn(ITERATION_TIMES, Optional.of(isNulls), floats);

    // Double columns
    double[] doubles = new double[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      doubles[i] = i;
    }
    columns[3] = new DoubleColumn(ITERATION_TIMES, Optional.of(isNulls), doubles);

    // Boolean columns
    boolean[] booleans = new boolean[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      booleans[i] = i % 2 == 0;
    }
    columns[4] = new BooleanColumn(ITERATION_TIMES, Optional.of(isNulls), booleans);

    // Binary columns
    Binary[] binaries = new Binary[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      binaries[i] = BytesUtils.valueOf(String.valueOf(i));
    }
    columns[5] = new BinaryColumn(ITERATION_TIMES, Optional.of(isNulls), binaries);

    // Another binary columns
    columns[6] = new BinaryColumn(ITERATION_TIMES, Optional.of(isNulls), binaries.clone());

    // The last time columns
    long[] times = new long[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      times[i] = i;
    }
    columns[7] = new TimeColumn(ITERATION_TIMES, times);

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

  private void testOrderedAccessByIndex() {
    try {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        if (i % 7 == 0) {
          assertTrue(rowRecordList.fieldsHasAnyNull(i));
        } else {
          assertFalse(rowRecordList.fieldsHasAnyNull(i));
          testRowRecord(rowRecordList.getRowRecord(i), i);
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
    Random random = new Random();

    try {
      for (int i = 0; i < ITERATION_TIMES; i++) {
        if (i % 7 == 0) {
          rowRecordList.put(generateRowRecordWithAllNullFields(i));
        } else {
          rowRecordList.put(
              generateRowRecord(i, byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin)));
        }
      }
      rowRecordList.setEvictionUpperBound(rowRecordList.size());
      for (int i = 0; i < ITERATION_TIMES; i++) {
        if (i % 7 == 0) {
          assertTrue(rowRecordList.fieldsHasAnyNull(i));
        } else {
          assertFalse(rowRecordList.fieldsHasAnyNull(i));
        }
      }

      byteLengthMin = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 16;
      byteLengthMax = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 32;
      for (int i = 0; i < ITERATION_TIMES; i++) {
        if (i % 7 == 0) {
          rowRecordList.put(generateRowRecordWithAllNullFields(i));
        } else {
          rowRecordList.put(
              generateRowRecord(i, byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin)));
        }
      }
      rowRecordList.setEvictionUpperBound(rowRecordList.size());
      for (int i = 0; i < ITERATION_TIMES; i++) {
        if (i % 7 == 0) {
          assertTrue(rowRecordList.fieldsHasAnyNull(i + ITERATION_TIMES));
        } else {
          assertFalse(rowRecordList.fieldsHasAnyNull(i + ITERATION_TIMES));
        }
      }

      byteLengthMin = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 256;
      byteLengthMax = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL * 512;
      for (int i = 0; i < ITERATION_TIMES; i++) {
        if (i % 7 == 0) {
          rowRecordList.put(generateRowRecordWithAllNullFields(i));
        } else {
          rowRecordList.put(
              generateRowRecord(i, byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin)));
        }
      }
      rowRecordList.setEvictionUpperBound(rowRecordList.size());
      for (int i = 0; i < ITERATION_TIMES; i++) {
        if (i % 7 == 0) {
          assertTrue(rowRecordList.fieldsHasAnyNull(i + 2 * ITERATION_TIMES));
        } else {
          assertFalse(rowRecordList.fieldsHasAnyNull(i + 2 * ITERATION_TIMES));
        }
      }

      for (int i = 0; i < 2 * ITERATION_TIMES; i++) {
        if (i % 7 == 0) {
          rowRecordList.put(generateRowRecordWithAllNullFields(i));
        } else {
          rowRecordList.put(
              generateRowRecord(i, byteLengthMin + random.nextInt(byteLengthMax - byteLengthMin)));
        }
        rowRecordList.setEvictionUpperBound(rowRecordList.size());
      }

      for (int i = 0; i < ITERATION_TIMES; i++) {
        if (i % 7 == 0) {
          assertTrue(rowRecordList.fieldsHasAnyNull(i + 3 * ITERATION_TIMES));
        } else {
          assertFalse(rowRecordList.fieldsHasAnyNull(i + 3 * ITERATION_TIMES));
        }
      }

      assertEquals(ITERATION_TIMES * 5, rowRecordList.size());
    } catch (QueryProcessException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private Object[] generateRowRecord(int time, int byteLength) {
    Object[] rowRecord = new Object[DATA_TYPES.length + 1];
    rowRecord[DATA_TYPES.length] = (long) time;
    for (int i = 0; i < DATA_TYPES.length; ++i) {
      switch (DATA_TYPES[i]) {
        case INT32:
          rowRecord[i] = time;
          break;
        case INT64:
          rowRecord[i] = (long) time;
          break;
        case FLOAT:
          rowRecord[i] = (float) time;
          break;
        case DOUBLE:
          rowRecord[i] = (double) time;
          break;
        case BOOLEAN:
          rowRecord[i] = time % 2 == 0;
          break;
        case TEXT:
          rowRecord[i] = BytesUtils.valueOf(generateRandomString(byteLength));
          break;
      }
    }
    return rowRecord;
  }

  private Object[] generateRowRecordWithAllNullFields(int time) {
    Object[] rowRecord = new Object[DATA_TYPES.length + 1];
    rowRecord[DATA_TYPES.length] = (long) time;
    return rowRecord;
  }

  private String generateRandomString(int length) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < length; ++i) {
      stringBuilder.append('.');
    }
    return stringBuilder.toString();
  }
}
