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

import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.template.MeasurementNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionSyntaxConventionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void createTimeSeriesTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String storageGroup = "root.sg";
      session.setStorageGroup(storageGroup);

      try {
        session.createTimeseries(
            "root.sg.d1.`a.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("is not a legal path"));
      }

      try {
        session.createTimeseries(
            "root.sg.d1.a`.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("is not a legal path"));
      }

      final SessionDataSet dataSet = session.executeQueryStatement("SHOW TIMESERIES");
      assertFalse(dataSet.hasNext());

      session.deleteStorageGroup(storageGroup);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();

      measurements.add("`\"a“（Φ）”b\"`");
      measurements.add("`\"a>b\"`");
      measurements.add("```a.b```");
      measurements.add("`'a“（Φ）”b'`");
      measurements.add("`'a>b'`");
      measurements.add("`a“（Φ）”b`");
      measurements.add("`a>b`");
      measurements.add("`\\\"a`");
      measurements.add("`aaa`");
      List<String> values = new ArrayList<>();

      for (int i = 0; i < measurements.size(); i++) {
        values.add("1");
      }

      session.insertRecord(deviceId, 1L, measurements, values);

      assertTrue(session.checkTimeseriesExists("root.sg1.d1.`\"a“（Φ）”b\"`"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.`\"a>b\"`"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.```a.b```"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.`'a“（Φ）”b'`"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.`'a>b'`"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.`a“（Φ）”b`"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.`a>b`"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.`\\\"a`"));
      assertTrue(session.checkTimeseriesExists("root.sg1.d1.aaa"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void inserRecordWithIllegalMeasurementTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      List<String> values = new ArrayList<>();
      measurements.add("a.b");
      values.add("1");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        e.printStackTrace();
      }

      measurements.clear();
      values.clear();
      measurements.add("111");
      values.add("1.2");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        e.printStackTrace();
      }

      measurements.clear();
      values.clear();
      measurements.add("`a");
      values.add("1.2");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        e.printStackTrace();
      }

      measurements.clear();
      values.clear();
      measurements.add("a..b");
      values.add("1.2");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        e.printStackTrace();
      }

      measurements.clear();
      values.clear();
      measurements.add("");
      values.add("1.2");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        e.printStackTrace();
      }

      measurements.clear();
      values.clear();
      measurements.add("`a``");
      values.add("1.2");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        e.printStackTrace();
      }

      measurements.clear();
      values.clear();
      measurements.add("`ab````");
      values.add("1.2");
      try {
        session.insertRecord(deviceId, 1L, measurements, values);
      } catch (Exception e) {
        e.printStackTrace();
      }

      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertRecordsWithIllegalMeasurementTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      measurements.add("a.b");
      measurements.add("111");
      measurements.add("`a");
      measurements.add("a..b");

      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("1.2");
      values.add("true");
      values.add("dad");

      List<Long> times = new ArrayList<>();
      times.add(1L);
      try {
        session.insertRecords(
            Collections.singletonList(deviceId),
            times,
            Collections.singletonList(measurements),
            Collections.singletonList(values));
      } catch (Exception e) {
        e.printStackTrace();
      }

      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertTabletWithIllegalMeasurementTest() {
    String deviceId = "root.sg1.d1";
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("wrong`", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
    schemaList.add(new MeasurementSchema("s4", TSDataType.INT64, TSEncoding.PLAIN));

    Tablet tablet = new Tablet(deviceId, schemaList, 10);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 10; time++) {
      int row = tablet.getRowSize();
      tablet.addTimestamp(row, time);
      long[] sensor = (long[]) values[0];
      sensor[row] = time;
      double[] sensor2 = (double[]) values[1];
      sensor2[row] = 0.1 + time;
      Binary[] sensor3 = (Binary[]) values[2];
      sensor3[row] = BytesUtils.valueOf("ha" + time);
      long[] sensor4 = (long[]) values[3];
      sensor4[row] = time;
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try {
        session.insertTablet(tablet);
        fail();
      } catch (Exception e) {
        e.printStackTrace();
      }

      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void createAlignedTimeseriesWithIllegalMeasurementTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      measurements.add("111");
      measurements.add("a.b");
      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.INT64);
      dataTypes.add(TSDataType.INT64);
      List<TSEncoding> encodings = new ArrayList<>();
      List<CompressionType> compressors = new ArrayList<>();
      for (int i = 1; i <= 2; i++) {
        encodings.add(TSEncoding.RLE);
        compressors.add(CompressionType.SNAPPY);
      }
      try {
        session.createAlignedTimeseries(
            deviceId, measurements, dataTypes, encodings, compressors, null, null, null);
      } catch (Exception e) {
        e.printStackTrace();
      }
      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void createAlignedTimeseriesWithIllegalMeasurementAliasTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");
      List<String> measurementAliasList = new ArrayList<>();
      measurementAliasList.add("s3");
      measurementAliasList.add("111");
      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.INT64);
      dataTypes.add(TSDataType.INT64);
      List<TSEncoding> encodings = new ArrayList<>();
      List<CompressionType> compressors = new ArrayList<>();
      for (int i = 1; i <= 2; i++) {
        encodings.add(TSEncoding.RLE);
        compressors.add(CompressionType.SNAPPY);
      }
      try {
        session.createAlignedTimeseries(
            deviceId,
            measurements,
            dataTypes,
            encodings,
            compressors,
            measurementAliasList,
            null,
            null);
      } catch (Exception e) {
        e.printStackTrace();
      }
      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void createMultiTimeseriesWithIllegalMeasurementAliasTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<String> paths = new ArrayList<>();
      paths.add("root.sg1.d1.s1");
      paths.add("root.sg1.d1.s2");
      List<TSDataType> tsDataTypes = new ArrayList<>();
      tsDataTypes.add(TSDataType.DOUBLE);
      tsDataTypes.add(TSDataType.DOUBLE);
      List<TSEncoding> tsEncodings = new ArrayList<>();
      tsEncodings.add(TSEncoding.RLE);
      tsEncodings.add(TSEncoding.RLE);
      List<CompressionType> compressionTypes = new ArrayList<>();
      compressionTypes.add(CompressionType.SNAPPY);
      compressionTypes.add(CompressionType.SNAPPY);
      List<String> measurementAliasList = new ArrayList<>();
      measurementAliasList.add("s3");
      measurementAliasList.add("111");

      try {
        session.createMultiTimeseries(
            paths,
            tsDataTypes,
            tsEncodings,
            compressionTypes,
            null,
            null,
            null,
            measurementAliasList);
      } catch (StatementExecutionException e) {
        e.printStackTrace();
      }
      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void createSchemaTemplateWithIllegalMeasurementTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String tempName = "flatTemplate";
      List<String> measurements = Arrays.asList("1", "2", "a.b");
      List<TSDataType> dataTypes =
          Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.DOUBLE);
      List<TSEncoding> encodings =
          Arrays.asList(TSEncoding.RLE, TSEncoding.RLE, TSEncoding.GORILLA);
      List<CompressionType> compressors =
          Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.LZ4);

      try {
        session.createSchemaTemplate(
            tempName, measurements, dataTypes, encodings, compressors, true);
      } catch (Exception e) {
        e.printStackTrace();
      }
      SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
      assertFalse(dataSet.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void createSchemaTemplateWithIllegalMeasurementNameTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {

      Template template = new Template("null_t", true);
      MeasurementNode mNode =
          new MeasurementNode(null, TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.LZ4);
      template.addToTemplate(mNode);

      try {
        session.createSchemaTemplate(template);
        fail();
      } catch (StatementExecutionException e) {
        TSStatusCode statusCode =
            TSStatusCode.representOf(TSStatusCode.METADATA_ERROR.getStatusCode());
        String message =
            String.format(
                "[%s] Exception occurred: %s failed. %s",
                statusCode,
                OperationType.CREATE_SCHEMA_TEMPLATE,
                "The name of a measurement in schema template shall not be null.");
        assertEquals(TSStatusCode.METADATA_ERROR.getStatusCode() + ": " + message, e.getMessage());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
