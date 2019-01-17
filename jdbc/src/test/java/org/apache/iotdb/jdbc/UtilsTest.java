/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.iotdb.service.rpc.thrift.TSDataValue;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSRowRecord;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UtilsTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testParseURL() throws IoTDBURLException {
    String userName = "test";
    String userPwd = "test";
    String host = "localhost";
    int port = 6667;
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, userName);
    properties.setProperty(Config.AUTH_PASSWORD, userPwd);
    IoTDBConnectionParams params = Utils
        .parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s/", host, port),
            properties);
    assertEquals(params.getHost(), host);
    assertEquals(params.getPort(), port);
    assertEquals(params.getUsername(), userName);
    assertEquals(params.getPassword(), userPwd);
  }

  @Test
  public void testVerifySuccess() {
    try {
      Utils.verifySuccess(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      fail();
    }

    try {
      Utils.verifySuccess(new TS_Status(TS_StatusCode.ERROR_STATUS));
    } catch (Exception e) {
      return;
    }
    fail();
  }

  @Test
  public void testConvertRowRecords() {
    final int DATA_TYPE_NUM = 6;
    Object[][] input = {
        {100L, "sensor1_boolean", TSDataType.BOOLEAN, false, "sensor1_int32", TSDataType.INT32,
            100, "sensor1_int64", TSDataType.INT64, 9999999999L, "sensor1_float", TSDataType.FLOAT,
            1.23f,
            "sensor1_double", TSDataType.DOUBLE, 1004234.435d, "sensor1_text", TSDataType.TEXT,
            "iotdb-jdbc",},
        {200L, "sensor2_boolean", TSDataType.BOOLEAN, true, "sensor2_int32", TSDataType.INT32, null,
            "sensor2_int64", TSDataType.INT64, -9999999999L, "sensor2_float", TSDataType.FLOAT,
            null,
            "sensor2_double", TSDataType.DOUBLE, -1004234.435d, "sensor2_text", TSDataType.TEXT,
            null,},
        {300L, "sensor3_boolean", TSDataType.BOOLEAN, null, "sensor3_int32", TSDataType.INT32, -100,
            "sensor3_int64", TSDataType.INT64, null, "sensor3_float", TSDataType.FLOAT, -1.23f,
            "sensor3_double", TSDataType.DOUBLE, null, "sensor3_text", TSDataType.TEXT,
            "jdbc-iotdb",},};
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet(new ArrayList<>());
    for (Object[] item : input) {
      TSRowRecord record = new TSRowRecord();
      record.setTimestamp((long) item[0]);
      List<String> keys = new ArrayList<>();
      List<TSDataValue> values = new ArrayList<>();
      for (int i = 0; i < DATA_TYPE_NUM; i++) {
        keys.add((String) item[3 * i + 1]);
        TSDataValue value = new TSDataValue(false);
        if (item[3 * i + 3] == null) {
          value.setIs_empty(true);
        } else {
          if (i == 0) {
            value.setBool_val((boolean) item[3 * i + 3]);
            value.setType(((TSDataType) item[3 * i + 2]).toString());
          } else if (i == 1) {
            value.setInt_val((int) item[3 * i + 3]);
            value.setType(((TSDataType) item[3 * i + 2]).toString());
          } else if (i == 2) {
            value.setLong_val((long) item[3 * i + 3]);
            value.setType(((TSDataType) item[3 * i + 2]).toString());
          } else if (i == 3) {
            value.setFloat_val((float) item[3 * i + 3]);
            value.setType(((TSDataType) item[3 * i + 2]).toString());
          } else if (i == 4) {
            value.setDouble_val((double) item[3 * i + 3]);
            value.setType(((TSDataType) item[3 * i + 2]).toString());
          } else {
            value.setBinary_val(ByteBuffer.wrap(((String) item[3 * i + 3]).getBytes()));
            value.setType(((TSDataType) item[3 * i + 2]).toString());
          }
        }
        values.add(value);
      }
      record.setValues(values);
      tsQueryDataSet.getRecords().add(record);
    }
    List<RowRecord> convertlist = Utils.convertRowRecords(tsQueryDataSet);
    int index = 0;
    for (RowRecord r : convertlist) {
      assertEquals(input[index][0], r.getTimestamp());
      List<Field> fields = r.getFields();
      int j = 0;
      for (Field f : fields) {
        if (j == 0) {
          if (input[index][3 * j + 3] == null) {
            assertTrue(f.isNull());
          } else {
            assertEquals(input[index][3 * j + 3], f.getBoolV());
          }
        } else if (j == 1) {
          if (input[index][3 * j + 3] == null) {
            assertTrue(f.isNull());
          } else {
            assertEquals(input[index][3 * j + 3], f.getIntV());
          }
        } else if (j == 2) {
          if (input[index][3 * j + 3] == null) {
            assertTrue(f.isNull());
          } else {
            assertEquals(input[index][3 * j + 3], f.getLongV());
          }
        } else if (j == 3) {
          if (input[index][3 * j + 3] == null) {
            assertTrue(f.isNull());
          } else {
            assertEquals(input[index][3 * j + 3], f.getFloatV());
          }
        } else if (j == 4) {
          if (input[index][3 * j + 3] == null) {
            assertTrue(f.isNull());
          } else {
            assertEquals(input[index][3 * j + 3], f.getDoubleV());
          }
        } else {
          if (input[index][3 * j + 3] == null) {
            assertTrue(f.isNull());
          } else {
            assertEquals(input[index][3 * j + 3], f.getStringValue());
          }
        }
        j++;
      }
      index++;
    }
  }

}
