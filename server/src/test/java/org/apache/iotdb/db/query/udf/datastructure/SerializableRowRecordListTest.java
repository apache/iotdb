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

package org.apache.iotdb.db.query.udf.datastructure;

import org.apache.iotdb.db.query.udf.datastructure.row.SerializableRowRecordList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
      generateData(i);
    }
    serializeAndDeserializeOnce();
    serializeAndDeserializeOnce();
    originalList.clear();
    testList.release();
    testList.init();
    for (int i = 0; i < ITERATION_TIMES; ++i) {
      generateData(i);
    }
    serializeAndDeserializeOnce();
    serializeAndDeserializeOnce();
  }

  protected void generateData(int index) {
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
          rowRecord.addField(Binary.valueOf(String.valueOf(index)), dataType);
          break;
      }
    }
    originalList.add(rowRecord);
    testList.put(convertRowRecordToRowInObjects(rowRecord));
  }

  protected Object[] convertRowRecordToRowInObjects(RowRecord rowRecord) {
    Object[] rowInObjects = new Object[rowRecord.getFields().size() + 1];
    rowInObjects[rowRecord.getFields().size()] = rowRecord.getTimestamp();
    for (int i = 0; i < rowRecord.getFields().size(); ++i) {
      switch (rowRecord.getFields().get(i).getDataType()) {
        case INT32:
          rowInObjects[i] = rowRecord.getFields().get(i).getIntV();
          break;
        case INT64:
          rowInObjects[i] = rowRecord.getFields().get(i).getLongV();
          break;
        case FLOAT:
          rowInObjects[i] = rowRecord.getFields().get(i).getFloatV();
          break;
        case DOUBLE:
          rowInObjects[i] = rowRecord.getFields().get(i).getDoubleV();
          break;
        case BOOLEAN:
          rowInObjects[i] = rowRecord.getFields().get(i).getBoolV();
          break;
        case TEXT:
          rowInObjects[i] = rowRecord.getFields().get(i).getBinaryV();
          break;
      }
    }
    return rowInObjects;
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
      Object[] testFields = testList.getRowRecord(i);
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
