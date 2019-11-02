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
package org.apache.iotdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.TSStatusType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
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
    String host1 = "localhost";
    int port = 6667;
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, userName);
    properties.setProperty(Config.AUTH_PASSWORD, userPwd);
    IoTDBConnectionParams params = Utils
        .parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s/", host1, port),
            properties);
    assertEquals(params.getHost(), host1);
    assertEquals(params.getPort(), port);
    assertEquals(params.getUsername(), userName);
    assertEquals(params.getPassword(), userPwd);

    //don't contain / in the end of url
    params = Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s", host1, port),
            properties);
    assertEquals(params.getHost(), host1);
    assertEquals(params.getPort(), port);

    //use a domain
    String host2 = "google.com";
    params = Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s", host2, port),
        properties);
    assertEquals(params.getHost(), host2);
    assertEquals(params.getPort(), port);

    //use a different domain
    String host3 = "www.google.com";
    params = Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s", host3, port),
        properties);
    assertEquals(params.getHost(), host3);
    assertEquals(params.getPort(), port);

    //use a ip
    String host4 = "1.2.3.4";
    params = Utils.parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s", host4, port),
        properties);
    assertEquals(params.getHost(), host4);
    assertEquals(params.getPort(), port);
  }

  @Test(expected = NumberFormatException.class)
  public void testParseWrongDomain() throws IoTDBURLException {
    String userName = "test";
    String userPwd = "test";
    String host = "www.::google.com";
    int port = 6667;
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, userName);
    properties.setProperty(Config.AUTH_PASSWORD, userPwd);
    IoTDBConnectionParams params = Utils
        .parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s/", host, port),
            properties);
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseWrongIP() throws IoTDBURLException {
    String userName = "test";
    String userPwd = "test";
    String host = "1.2.3.";
    int port = 6667;
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, userName);
    properties.setProperty(Config.AUTH_PASSWORD, userPwd);
    IoTDBConnectionParams params = Utils
        .parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s/", host, port),
            properties);
  }

  @Test(expected = IoTDBURLException.class)
  public void testParseWrongPort() throws IoTDBURLException {
    String userName = "test";
    String userPwd = "test";
    String host = "localhost";
    int port = 66699999;
    Properties properties = new Properties();
    properties.setProperty(Config.AUTH_USER, userName);
    properties.setProperty(Config.AUTH_PASSWORD, userPwd);
    IoTDBConnectionParams params = Utils
        .parseUrl(String.format(Config.IOTDB_URL_PREFIX + "%s:%s/", host, port),
            properties);
  }

  @Test
  public void testVerifySuccess() {
    try {
      TSStatusType successStatus = new TSStatusType(TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          "");
      RpcUtils.verifySuccess(new TSStatus(successStatus));
    } catch (Exception e) {
      fail();
    }

    try {
      TSStatusType errorStatus = new TSStatusType(
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), "");
      RpcUtils.verifySuccess(new TSStatus(errorStatus));
    } catch (Exception e) {
      return;
    }
    fail();
  }

  @Test
  public void testConvertRowRecords() throws IOException {
    Object[][] input = {
        {100L, "sensor1_boolean", TSDataType.BOOLEAN, false,
            "sensor1_int32", TSDataType.INT32, 100,
            "sensor1_int64", TSDataType.INT64, 9999999999L,
            "sensor1_float", TSDataType.FLOAT, 1.23f,
            "sensor1_double", TSDataType.DOUBLE, 1004234.435d,
            "sensor1_text", TSDataType.TEXT, "iotdb-jdbc",},
        {200L, "sensor2_boolean", TSDataType.BOOLEAN, true,
            "sensor2_int32", null, null,
            "sensor2_int64", TSDataType.INT64, -9999999999L,
            "sensor2_float", null, null,
            "sensor2_double", TSDataType.DOUBLE, -1004234.435d,
            "sensor2_text", null, null,},
        {300L, "sensor3_boolean", null, null,
            "sensor3_int32", TSDataType.INT32, -100,
            "sensor3_int64", null, null,
            "sensor3_float", TSDataType.FLOAT, -1.23f,
            "sensor3_double", null, null,
            "sensor3_text", TSDataType.TEXT, "jdbc-iotdb",},};

    // create the TSQueryDataSet in the similar way as the server does
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
    int rowCount = input.length;
    tsQueryDataSet.setRowCount(rowCount);
    int columnNum = 6;
    int columnNumWithTime = columnNum + 1;
    DataOutputStream[] dataOutputStreams = new DataOutputStream[columnNumWithTime];
    ByteArrayOutputStream[] byteArrayOutputStreams = new ByteArrayOutputStream[columnNumWithTime];
    for (int i = 0; i < columnNumWithTime; i++) {
      byteArrayOutputStreams[i] = new ByteArrayOutputStream();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
    }
    int valueOccupation = 0;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = input[i];
      // use columnOutput to write byte array
      dataOutputStreams[0].writeLong((long) row[0]);
      for (int k = 0; k < columnNum; k++) {
        DataOutputStream dataOutputStream = dataOutputStreams[k + 1]; // DO NOT FORGET +1
        Object type = row[1 + 3 * k + 1];
        Object value = row[1 + 3 * k + 2];
        if (type == null) {
          dataOutputStream.writeBoolean(true); // is_empty true
        } else {
          dataOutputStream.writeBoolean(false); // is_empty false
          TSDataType dataType = (TSDataType) type;
          switch (dataType) {
            case INT32:
              dataOutputStream.writeInt((int) value);
              valueOccupation += 4;
              break;
            case INT64:
              dataOutputStream.writeLong((long) value);
              valueOccupation += 8;
              break;
            case FLOAT:
              dataOutputStream.writeFloat((float) value);
              valueOccupation += 4;
              break;
            case DOUBLE:
              dataOutputStream.writeDouble((double) value);
              valueOccupation += 8;
              break;
            case BOOLEAN:
              dataOutputStream.writeBoolean((boolean) value);
              valueOccupation += 1;
              break;
            case TEXT:
              Binary binaryValue = new Binary((String) value);
              dataOutputStream.writeInt(binaryValue.getLength());
              dataOutputStream.write(binaryValue.getValues());
              valueOccupation = valueOccupation + 4 + binaryValue.getLength();
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }
        }
      }
    }
    // calculate total valueOccupation
    valueOccupation += rowCount * 8; // note the timestamp column needn't the boolean is_empty
    valueOccupation += rowCount * columnNum; // for all is_empty
    ByteBuffer valueBuffer = ByteBuffer.allocate(valueOccupation);
    for (ByteArrayOutputStream byteArrayOutputStream : byteArrayOutputStreams) {
      valueBuffer.put(byteArrayOutputStream.toByteArray());
    }
    valueBuffer.flip(); // PAY ATTENTION TO HERE
    tsQueryDataSet.setValues(valueBuffer);
    tsQueryDataSet.setRowCount(rowCount);

    // begin convert
    List<String> columnTypeList = new ArrayList<>();
    columnTypeList.add(TSDataType.BOOLEAN.toString());
    columnTypeList.add(TSDataType.INT32.toString());
    columnTypeList.add(TSDataType.INT64.toString());
    columnTypeList.add(TSDataType.FLOAT.toString());
    columnTypeList.add(TSDataType.DOUBLE.toString());
    columnTypeList.add(TSDataType.TEXT.toString());
    List<RowRecord> convertlist = Utils.convertRowRecords(tsQueryDataSet, columnTypeList);
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
