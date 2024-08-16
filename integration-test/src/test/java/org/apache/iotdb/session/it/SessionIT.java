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
package org.apache.iotdb.session.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class SessionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsertByStrAndSelectFailedData() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";

      session.createTimeseries(
          deviceId + ".s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
      session.createTimeseries(
          deviceId + ".s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
      session.createTimeseries(
          deviceId + ".s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
      session.createTimeseries(
          deviceId + ".s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.UNCOMPRESSED);

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
      schemaList.add(new MeasurementSchema("s4", TSDataType.INT64, TSEncoding.PLAIN));

      Tablet tablet = new Tablet("root.sg1.d1", schemaList, 10);

      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;

      for (long time = 0; time < 10; time++) {
        int row = tablet.rowSize++;
        timestamps[row] = time;
        long[] sensor = (long[]) values[0];
        sensor[row] = time;
        double[] sensor2 = (double[]) values[1];
        sensor2[row] = 0.1 + time;
        Binary[] sensor3 = (Binary[]) values[2];
        sensor3[row] = BytesUtils.valueOf("ha" + time);
        long[] sensor4 = (long[]) values[3];
        sensor4[row] = time;
      }

      try {
        session.insertTablet(tablet);
        fail();
      } catch (StatementExecutionException e) {
        // ignore
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select s1, s2, s3, s4 from root.sg1.d1");
      int i = 0;
      while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
        Assert.assertEquals(i, record.getFields().get(0).getLongV());
        Assert.assertNull(record.getFields().get(1).getDataType());
        Assert.assertNull(record.getFields().get(2).getDataType());
        Assert.assertEquals(i, record.getFields().get(3).getDoubleV(), 0.00001);
        i++;
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertRecord() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<TSDataType> dataTypeList =
          Arrays.asList(TSDataType.DATE, TSDataType.TIMESTAMP, TSDataType.BLOB, TSDataType.STRING);
      List<String> measurements = Arrays.asList("s1", "s2", "s3", "s4");
      String deviceId = "root.db.d1";
      for (int i = 0; i < dataTypeList.size(); i++) {
        String tsPath = deviceId + "." + measurements.get(i);
        if (!session.checkTimeseriesExists(tsPath)) {
          session.createTimeseries(
              tsPath, dataTypeList.get(i), TSEncoding.PLAIN, CompressionType.SNAPPY);
        }
      }
      byte[] bytes = new byte[2];
      bytes[0] = (byte) Integer.parseInt("BA", 16);
      bytes[1] = (byte) Integer.parseInt("BE", 16);
      for (long time = 10; time < 20; time++) {
        List<Object> values = new ArrayList<>();
        values.add(LocalDate.of(2024, 1, (int) time));
        values.add(time);
        values.add(new Binary(bytes));
        values.add("" + time);
        session.insertRecord(deviceId, time, measurements, dataTypeList, values);
      }
      try (SessionDataSet dataSet = session.executeQueryStatement("select * from root.db.d1")) {
        HashSet<String> columnNames = new HashSet<>(dataSet.getColumnNames());
        Assert.assertEquals(5, columnNames.size());
        for (int i = 0; i < 4; i++) {
          Assert.assertTrue(columnNames.contains(deviceId + "." + measurements.get(i)));
        }
        dataSet.setFetchSize(1024); // default is 10000
        int row = 10;
        while (dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          Assert.assertEquals(row, record.getTimestamp());
          List<Field> fields = record.getFields();
          Assert.assertEquals(4, fields.size());
          for (int i = 0; i < 4; i++) {
            switch (fields.get(i).getDataType()) {
              case DATE:
                Assert.assertEquals(LocalDate.of(2024, 1, row), fields.get(i).getDateV());
                break;
              case TIMESTAMP:
                Assert.assertEquals(row, fields.get(i).getLongV());
                break;
              case BLOB:
                Assert.assertArrayEquals(bytes, fields.get(i).getBinaryV().getValues());
                break;
              case STRING:
                Assert.assertEquals("" + row, fields.get(i).getBinaryV().toString());
                break;
              default:
                fail("Unsupported data type");
            }
            fields.get(i).getDataType();
          }
          row++;
        }
        Assert.assertEquals(20, row);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertStrRecord() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<TSDataType> dataTypeList =
          Arrays.asList(TSDataType.DATE, TSDataType.TIMESTAMP, TSDataType.BLOB, TSDataType.STRING);
      List<String> measurements = Arrays.asList("s1", "s2", "s3", "s4");
      String deviceId = "root.db.d1";
      for (int i = 0; i < dataTypeList.size(); i++) {
        String tsPath = deviceId + "." + measurements.get(i);
        if (!session.checkTimeseriesExists(tsPath)) {
          session.createTimeseries(
              tsPath, dataTypeList.get(i), TSEncoding.PLAIN, CompressionType.SNAPPY);
        }
      }
      byte[] bytes = new byte[2];
      bytes[0] = (byte) Integer.parseInt("BA", 16);
      bytes[1] = (byte) Integer.parseInt("BE", 16);
      for (long time = 10; time < 20; time++) {
        List<String> values = new ArrayList<>();
        values.add("2024-01-" + time);
        values.add("" + time);
        values.add("X'BABE'");
        values.add("" + time);
        session.insertRecord(deviceId, time, measurements, values);
      }
      try (SessionDataSet dataSet = session.executeQueryStatement("select * from root.db.d1")) {
        HashSet<String> columnNames = new HashSet<>(dataSet.getColumnNames());
        Assert.assertEquals(5, columnNames.size());
        for (int i = 0; i < 4; i++) {
          Assert.assertTrue(columnNames.contains(deviceId + "." + measurements.get(i)));
        }
        dataSet.setFetchSize(1024); // default is 10000
        int row = 10;
        while (dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          System.out.println(record);
          Assert.assertEquals(row, record.getTimestamp());
          List<Field> fields = record.getFields();
          Assert.assertEquals(4, fields.size());
          for (int i = 0; i < 4; i++) {
            switch (fields.get(i).getDataType()) {
              case DATE:
                Assert.assertEquals(LocalDate.of(2024, 1, row), fields.get(i).getDateV());
                break;
              case TIMESTAMP:
                Assert.assertEquals(row, fields.get(i).getLongV());
                break;
              case BLOB:
                Assert.assertArrayEquals(bytes, fields.get(i).getBinaryV().getValues());
                break;
              case STRING:
                Assert.assertEquals("" + row, fields.get(i).getBinaryV().toString());
                break;
              default:
                fail("Unsupported data type");
            }
            fields.get(i).getDataType();
          }
          row++;
        }
        Assert.assertEquals(20, row);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertTablet() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      String deviceId = "root.db.d1";
      schemaList.add(new MeasurementSchema("s1", TSDataType.DATE));
      schemaList.add(new MeasurementSchema("s2", TSDataType.TIMESTAMP));
      schemaList.add(new MeasurementSchema("s3", TSDataType.BLOB));
      schemaList.add(new MeasurementSchema("s4", TSDataType.STRING));
      Tablet tablet = new Tablet(deviceId, schemaList, 100);
      byte[] bytes = new byte[2];
      bytes[0] = (byte) Integer.parseInt("BA", 16);
      bytes[1] = (byte) Integer.parseInt("BE", 16);
      // Method 1 to add tablet data
      for (long time = 10; time < 15; time++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, time);
        tablet.addValue(
            schemaList.get(0).getMeasurementId(), rowIndex, LocalDate.of(2024, 1, (int) time));
        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, time);
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, new Binary(bytes));
        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, "" + time);
      }
      session.insertTablet(tablet);
      tablet.reset();
      // Method 2 to add tablet data
      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;
      for (long time = 15; time < 20; time++) {
        int rowIndex = tablet.rowSize++;
        timestamps[rowIndex] = time;
        ((LocalDate[]) values[0])[rowIndex] = LocalDate.of(2024, 1, (int) time);
        ((long[]) values[1])[rowIndex] = time;
        ((Binary[]) values[2])[rowIndex] = new Binary(bytes);
        ((Binary[]) values[3])[rowIndex] = new Binary(time + "", TSFileConfig.STRING_CHARSET);
      }
      session.insertTablet(tablet);
      tablet.reset();
      try (SessionDataSet dataSet = session.executeQueryStatement("select * from root.db.d1")) {
        HashSet<String> columnNames = new HashSet<>(dataSet.getColumnNames());
        Assert.assertEquals(5, columnNames.size());
        for (int i = 0; i < 4; i++) {
          Assert.assertTrue(
              columnNames.contains(deviceId + "." + schemaList.get(i).getMeasurementId()));
        }
        dataSet.setFetchSize(1024); // default is 10000
        int row = 10;
        while (dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          Assert.assertEquals(row, record.getTimestamp());
          List<Field> fields = record.getFields();
          Assert.assertEquals(4, fields.size());
          for (int i = 0; i < 4; i++) {
            switch (fields.get(i).getDataType()) {
              case DATE:
                Assert.assertEquals(LocalDate.of(2024, 1, row), fields.get(i).getDateV());
                break;
              case TIMESTAMP:
                Assert.assertEquals(row, fields.get(i).getLongV());
                break;
              case BLOB:
                Assert.assertArrayEquals(bytes, fields.get(i).getBinaryV().getValues());
                break;
              case STRING:
                Assert.assertEquals("" + row, fields.get(i).getBinaryV().toString());
                break;
              default:
                fail("Unsupported data type");
            }
            fields.get(i).getDataType();
          }
          row++;
        }
        Assert.assertEquals(20, row);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
