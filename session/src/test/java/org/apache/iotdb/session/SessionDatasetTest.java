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

package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SessionDatasetTest {

  private static final String STORAGE_GROUP = "root.tbd";
  private static final String DEVICE_ID = "root.tbd.rd";
  private static final String ROOT_TsBLOCK_INT = STORAGE_GROUP + ".rd.testInt";
  private static final String ROOT_TsBLOCK_LONG = STORAGE_GROUP + ".rd.testLong";
  private static final String ROOT_TsBLOCK_BOOLEAN = STORAGE_GROUP + ".rd.testBoolean";
  private static final String ROOT_TsBLOCK_FLOAT = STORAGE_GROUP + ".rd.testFloat";
  private static final String ROOT_TsBLOCK_DOUBLE = STORAGE_GROUP + ".rd.testDouble";
  private static final String ROOT_TsBLOCK_TEXT = STORAGE_GROUP + ".rd.testText";
  private static final int ROW_NUMBER = 800000;

  private int INT_DATA;
  private long LONG_DATA;
  private boolean BOOLEAN_DATA;
  private float FLOAT_DATA;
  private double DOUBLE_DATA;
  private String TEXT_DATA;

  private final String USERNAME = "root";
  private final String PASSWORD = "root";
  private final String PORT = "6667";
  private final String IP = "127.0.0.1";

  private int[] intData;
  private float[] floatData;
  private double[] doubleData;
  private long[] longData;
  private boolean[] booleanData;
  private String[] textData;
  private long[] times;

  Session session;

  @Test
  public void testTsBlockRpcDataset() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session(IP, PORT, USERNAME, PASSWORD);
    session.open();
    session.setStorageGroup(STORAGE_GROUP);
    try {
      createTimeSeries();
      addData();
      testV2();
    } catch (Exception e) {
      e.printStackTrace();
    }
    session.deleteStorageGroup(STORAGE_GROUP);
    session.close();
  }

  public void createTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
    if (!session.checkTimeseriesExists(ROOT_TsBLOCK_INT)) {
      session.createTimeseries(
          ROOT_TsBLOCK_INT, TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_TsBLOCK_LONG)) {
      session.createTimeseries(
          ROOT_TsBLOCK_LONG, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_TsBLOCK_BOOLEAN)) {
      session.createTimeseries(
          ROOT_TsBLOCK_BOOLEAN, TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_TsBLOCK_FLOAT)) {
      session.createTimeseries(
          ROOT_TsBLOCK_FLOAT, TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_TsBLOCK_DOUBLE)) {
      session.createTimeseries(
          ROOT_TsBLOCK_DOUBLE, TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_TsBLOCK_TEXT)) {
      session.createTimeseries(
          ROOT_TsBLOCK_TEXT, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY);
    }
  }

  private void addData() throws IoTDBConnectionException, StatementExecutionException {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("testInt", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("testLong", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("testFloat", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("testDouble", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("testBoolean", TSDataType.BOOLEAN));
    schemaList.add(new MeasurementSchema("testText", TSDataType.TEXT));
    Tablet tablet = new Tablet(DEVICE_ID, schemaList, 100);

    intData = new int[ROW_NUMBER];
    floatData = new float[ROW_NUMBER];
    doubleData = new double[ROW_NUMBER];
    booleanData = new boolean[ROW_NUMBER];
    longData = new long[ROW_NUMBER];
    textData = new String[ROW_NUMBER];
    times = new long[ROW_NUMBER];

    for (int row = 0; row < ROW_NUMBER; row++) {

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }
    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  public void testV2() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet sd = session.executeQueryStatement("select * from " + DEVICE_ID, 100000);
    int r = 0;
    while (sd.hasNext()) {
      RowRecord row = sd.next();
      Assert.assertEquals(row.getTimestamp(), times[r]);
      List<Field> fields = row.getFields();
      for (Field field : fields) {
        try {
          switch (field.getDataType()) {
            case BOOLEAN:
              Assert.assertEquals(booleanData[r], field.getBoolV());
              break;
            case INT32:
              Assert.assertEquals(intData[r], field.getIntV());
              break;
            case INT64:
              Assert.assertEquals(longData[r], field.getLongV());
              break;
            case FLOAT:
              Assert.assertEquals(floatData[r], field.getFloatV(), 0.01);
              break;
            case DOUBLE:
              Assert.assertEquals(doubleData[r], field.getDoubleV(), 0.01);
              break;
            case TEXT:
              Assert.assertEquals(textData[r], field.getStringValue());
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", field.getDataType()));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      r++;
    }

    SessionDataSet sd2 =
        session.executeLastDataQuery(
            Collections.singletonList(ROOT_TsBLOCK_INT), times[ROW_NUMBER - 2], 10000);
    while (sd2.hasNext()) {
      RowRecord row = sd2.next();
      Assert.assertEquals(
          Integer.toString(intData[ROW_NUMBER - 1]), row.getFields().get(1).getStringValue());
    }

    r = 0;
    SessionDataSet sd3 =
        session.executeRawDataQuery(
            Collections.singletonList(ROOT_TsBLOCK_INT), times[0], times[ROW_NUMBER / 2], 10000);
    while (sd3.hasNext()) {
      RowRecord row = sd3.next();
      Assert.assertEquals(row.getTimestamp(), times[r]);
      List<Field> fields = row.getFields();
      for (Field field : fields) {
        try {
          switch (field.getDataType()) {
            case BOOLEAN:
              Assert.assertEquals(booleanData[r], field.getBoolV());
              break;
            case INT32:
              Assert.assertEquals(intData[r], field.getIntV());
              break;
            case INT64:
              Assert.assertEquals(longData[r], field.getLongV());
              break;
            case FLOAT:
              Assert.assertEquals(floatData[r], field.getFloatV(), 0.01);
              break;
            case DOUBLE:
              Assert.assertEquals(doubleData[r], field.getDoubleV(), 0.01);
              break;
            case TEXT:
              Assert.assertEquals(textData[r], field.getStringValue());
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", field.getDataType()));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      r++;
    }

    session.close();
  }
}
