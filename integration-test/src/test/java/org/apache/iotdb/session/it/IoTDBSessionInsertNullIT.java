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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionInsertNullIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSessionInsertNullIT.class);
  private final int retry = 30;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void prepareData(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    session.setStorageGroup("root.sg1");
    session.createTimeseries(
        "root.sg1.clsu.d1.s1", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.clsu.d1.s2", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.clsu.d1.s3", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.clsu.d1.s4", TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.clsu.d1.s5", TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.clsu.d1.s6", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.clsu.d2.s1", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
  }

  private long queryCountRecords(ISession session, String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSetWrapper = session.executeQueryStatement(sql, 60_000);
    long count = 0;
    while (dataSetWrapper.hasNext()) {
      RowRecord record = dataSetWrapper.next();
      Field field = record.getFields().get(0);
      switch (field.getDataType()) {
        case INT32:
          count = field.getIntV();
          break;
        case INT64:
          count = field.getLongV();
          break;
      }
    }
    return count;
  }

  @Test
  public void insertRecordNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      String deviceId = "root.sg1.clsu.d1";
      session.insertRecord(deviceId, 100, Arrays.asList("s1"), Arrays.asList("true"));
      List<String> t = new ArrayList<>();
      t.add(null);
      session.insertRecord(deviceId, 200, Arrays.asList("s1"), t);
      session.insertRecord(
          deviceId,
          300,
          Arrays.asList("s1", "s2"),
          Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
          Arrays.asList(true, 30));
      session.insertRecord(
          deviceId,
          400,
          Arrays.asList("s1", "s2"),
          Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
          Arrays.asList(true, null));
      session.insertRecord(
          deviceId,
          500,
          Arrays.asList("s1", "s2"),
          Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
          Arrays.asList(null, null));
      long nums = queryCountRecords(session, "select count(s1) from " + deviceId);
      assertEquals(3, nums);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertAlignedRecordNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      String deviceId = "root.sg1.clsu.aligned_d1";
      session.insertAlignedRecord(deviceId, 100, Arrays.asList("s1"), Arrays.asList("true"));
      List<String> t = new ArrayList<>();
      t.add(null);
      session.insertAlignedRecord(deviceId, 200, Arrays.asList("s1"), t);
      session.insertAlignedRecord(
          deviceId,
          300,
          Arrays.asList("s1", "s2"),
          Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
          Arrays.asList(true, 30));
      session.insertAlignedRecord(
          deviceId,
          400,
          Arrays.asList("s1", "s2"),
          Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
          Arrays.asList(true, null));
      session.insertAlignedRecord(
          deviceId,
          500,
          Arrays.asList("s1", "s2"),
          Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
          Arrays.asList(null, null));
      for (int i = 0; i < retry; i++) {
        try {
          long nums = queryCountRecords(session, "select count(s1) from " + deviceId);
          assertEquals(3, nums);
          return;
        } catch (Exception e) {
          LOGGER.info("query records failed, retry: " + i);
          Thread.sleep(1000);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertRecordsNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      String deviceId1 = "root.sg1.clsu.d2";
      String deviceId2 = "root.sg1.clsu.d3";
      session.insertRecords(
          Arrays.asList(deviceId1, deviceId2),
          Arrays.asList(300L, 300L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32)),
          Arrays.asList(Arrays.asList(true, 101), Arrays.asList(false, 201)));
      session.insertRecords(
          Arrays.asList(deviceId1, deviceId2),
          Arrays.asList(200L, 200L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(Arrays.asList("false", "101"), Arrays.asList("true", "201")));
      session.insertRecords(
          Arrays.asList(deviceId1, deviceId2),
          Arrays.asList(400L, 400L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(Arrays.asList(null, "102"), Arrays.asList("false", "202")));
      session.insertRecords(
          Arrays.asList(deviceId1, deviceId2),
          Arrays.asList(500L, 500L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32)),
          Arrays.asList(Arrays.asList(true, null), Arrays.asList(null, null)));

      for (int i = 0; i < retry; i++) {
        try {
          long nums = queryCountRecords(session, "select count(s1) from " + deviceId1);
          assertEquals(3, nums);
          nums = queryCountRecords(session, "select count(s2) from " + deviceId2);
          assertEquals(3, nums);
          return;
        } catch (Exception e) {
          LOGGER.info("query records failed, retry: " + i);
          Thread.sleep(1000);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertAlignedRecordsNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);
      String deviceId1 = "root.sg1.clsu.aligned_d2";
      String deviceId2 = "root.sg1.clsu.aligned_d3";
      session.insertAlignedRecords(
          Arrays.asList(deviceId1, deviceId2),
          Arrays.asList(300L, 300L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32)),
          Arrays.asList(Arrays.asList(true, 101), Arrays.asList(false, 201)));
      session.insertAlignedRecords(
          Arrays.asList(deviceId1, deviceId2),
          Arrays.asList(200L, 200L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(Arrays.asList("false", "101"), Arrays.asList("true", "201")));
      session.insertAlignedRecords(
          Arrays.asList(deviceId1, deviceId2),
          Arrays.asList(400L, 400L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(Arrays.asList(null, "102"), Arrays.asList("false", "202")));
      session.insertAlignedRecords(
          Arrays.asList(deviceId1, deviceId2),
          Arrays.asList(500L, 500L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32)),
          Arrays.asList(Arrays.asList(true, null), Arrays.asList(null, null)));
      long nums = queryCountRecords(session, "select count(s1) from " + deviceId1);
      assertEquals(3, nums);
      nums = queryCountRecords(session, "select count(s2) from " + deviceId2);
      assertEquals(3, nums);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertRecordsOfOneDeviceNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);
      String deviceId1 = "root.sg1.clsu.InsertRecordsOfOneDevice";
      session.insertRecordsOfOneDevice(
          deviceId1,
          Arrays.asList(300L, 301L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32)),
          Arrays.asList(Arrays.asList(true, 101), Arrays.asList(false, 201)));
      session.insertStringRecordsOfOneDevice(
          deviceId1,
          Arrays.asList(200L, 201L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(Arrays.asList("false", "101"), Arrays.asList("true", "201")));
      session.insertStringRecordsOfOneDevice(
          deviceId1,
          Arrays.asList(400L, 401L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(Arrays.asList(null, "102"), Arrays.asList("false", "202")));
      session.insertRecordsOfOneDevice(
          deviceId1,
          Arrays.asList(500L, 501L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32)),
          Arrays.asList(Arrays.asList(true, null), Arrays.asList(null, null)));
      long nums = queryCountRecords(session, "select count(s1) from " + deviceId1);
      assertEquals(6, nums);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertAlignedRecordsOfOneDeviceNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);
      String deviceId1 = "root.sg1.clsu.InsertAlignedRecordsOfOneDevice";
      session.insertAlignedRecordsOfOneDevice(
          deviceId1,
          Arrays.asList(300L, 301L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32)),
          Arrays.asList(Arrays.asList(true, 101), Arrays.asList(false, 201)));
      session.insertAlignedStringRecordsOfOneDevice(
          deviceId1,
          Arrays.asList(200L, 201L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(Arrays.asList("false", "101"), Arrays.asList("true", "201")));
      session.insertAlignedStringRecordsOfOneDevice(
          deviceId1,
          Arrays.asList(400L, 401L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(Arrays.asList(null, "102"), Arrays.asList("false", "202")));
      session.insertAlignedRecordsOfOneDevice(
          deviceId1,
          Arrays.asList(500L, 501L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32),
              Arrays.asList(TSDataType.BOOLEAN, TSDataType.INT32)),
          Arrays.asList(Arrays.asList(true, null), Arrays.asList(null, null)));
      long nums = queryCountRecords(session, "select count(s1) from " + deviceId1);
      assertEquals(6, nums);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
