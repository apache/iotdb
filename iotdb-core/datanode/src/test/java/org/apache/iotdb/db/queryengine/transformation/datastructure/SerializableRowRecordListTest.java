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

import org.apache.iotdb.db.queryengine.transformation.datastructure.row.SerializableRowRecordList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SerializableRowRecordListTest extends SerializableListTest {

  private static final TSDataType[] DATA_TYPES = {
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.BOOLEAN,
    TSDataType.TEXT
  };

  private List<RowRecord> originalList;
  private SerializableRowRecordList testList;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    originalList = new ArrayList<>();
    testList =
        SerializableRowRecordList.newSerializableRowRecordList(
            QUERY_ID, DATA_TYPES, INTERNAL_ROW_RECORD_LIST_CAPACITY);
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void serializeAndDeserializeTest() {
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      RowRecord row = generateRowRecord(i);
      originalList.add(row);
    }
    Column[] columns = generateColumns();
    testList.putColumns(columns);

    serializeAndDeserializeOnce();
    serializeAndDeserializeOnce();

    originalList.clear();
    testList.release();
    testList.init();
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      RowRecord row = generateRowRecord(i);
      originalList.add(row);
    }
    columns = generateColumns();
    testList.putColumns(columns);

    serializeAndDeserializeOnce();
    serializeAndDeserializeOnce();
  }

  protected RowRecord generateRowRecord(int index) {
    RowRecord rowRecord = new RowRecord(index);
    for (TSDataType dataType : DATA_TYPES) {
      switch (dataType) {
        case INT32:
          rowRecord.addField(index, dataType);
          break;
        case INT64:
          rowRecord.addField((long) index, dataType);
          break;
        case FLOAT:
          rowRecord.addField((float) index, dataType);
          break;
        case DOUBLE:
          rowRecord.addField((double) index, dataType);
          break;
        case BOOLEAN:
          rowRecord.addField(index % 2 == 0, dataType);
          break;
        case TEXT:
          rowRecord.addField(BytesUtils.valueOf(String.valueOf(index)), dataType);
          break;
      }
    }

    return rowRecord;
  }

  protected Column[] generateColumns() {
    Column[] columns = new Column[DATA_TYPES.length + 1];

    // Int columns
    int[] ints = new int[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      ints[i] = i;
    }
    columns[0] = new IntColumn(ITERATION_TIMES, Optional.empty(), ints);

    // Long columns
    long[] longs = new long[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      longs[i] = i;
    }
    columns[1] = new LongColumn(ITERATION_TIMES, Optional.empty(), longs);

    // Float columns
    float[] floats = new float[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      floats[i] = i;
    }
    columns[2] = new FloatColumn(ITERATION_TIMES, Optional.empty(), floats);

    // Double columns
    double[] doubles = new double[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      doubles[i] = i;
    }
    columns[3] = new DoubleColumn(ITERATION_TIMES, Optional.empty(), doubles);

    // Boolean columns
    boolean[] booleans = new boolean[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      booleans[i] = i % 2 == 0;
    }
    columns[4] = new BooleanColumn(ITERATION_TIMES, Optional.empty(), booleans);

    // Binary columns
    Binary[] binaries = new Binary[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      binaries[i] = BytesUtils.valueOf(String.valueOf(i));
    }
    columns[5] = new BinaryColumn(ITERATION_TIMES, Optional.empty(), binaries);

    // The last time columns
    long[] times = new long[ITERATION_TIMES];
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      times[i] = i;
    }
    columns[6] = new TimeColumn(ITERATION_TIMES, times);

    return columns;
  }

  protected void serializeAndDeserializeOnce() {
    try {
      testList.serialize();
    } catch (IOException e) {
      fail();
    }
    try {
      testList.deserialize();
    } catch (IOException e) {
      fail();
    }
    assertEquals(ITERATION_TIMES, testList.size());

    for (int i = 0; i < testList.size(); ++i) {
      assertEquals(originalList.get(i).getTimestamp(), testList.getTime(i));
      List<Field> originalFields = originalList.get(i).getFields();
      Object[] testFields = testList.getRow(i);
      for (int j = 0; j < DATA_TYPES.length; ++j) {
        switch (DATA_TYPES[j]) {
          case INT32:
            assertEquals(originalFields.get(j).getIntV(), (int) testFields[j]);
            break;
          case INT64:
            assertEquals(originalFields.get(j).getLongV(), (long) testFields[j]);
            break;
          case FLOAT:
            assertEquals(originalFields.get(j).getFloatV(), (float) testFields[j], 0);
            break;
          case DOUBLE:
            assertEquals(originalFields.get(j).getDoubleV(), (double) testFields[j], 0);
            break;
          case BOOLEAN:
            assertEquals(originalFields.get(j).getBoolV(), testFields[j]);
            break;
          case TEXT:
            assertEquals(originalFields.get(j).getBinaryV(), testFields[j]);
            break;
        }
      }
    }
  }
}
