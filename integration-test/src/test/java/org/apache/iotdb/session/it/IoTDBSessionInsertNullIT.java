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
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionInsertNullIT {

  private static final AtomicInteger NEXT_DATABASE_ID = new AtomicInteger();

  private String database;

  @BeforeClass
  public static void setUpClass() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void setUp() {
    // Keep each method's schema and data independent while sharing the same cluster.
    database = "root.insert_null_" + NEXT_DATABASE_ID.incrementAndGet();
  }

  @AfterClass
  public static void tearDownClass() {
    // All method-specific databases are bounded by this class and removed with the cluster.
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void prepareData(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    session.setStorageGroup(database);
    session.createTimeseries(
        database + ".clsu.d1.s1", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        database + ".clsu.d1.s2", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        database + ".clsu.d1.s3", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        database + ".clsu.d1.s4", TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        database + ".clsu.d1.s5", TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        database + ".clsu.d1.s6", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        database + ".clsu.d2.s1", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
  }

  private long queryCountRecords(ISession session, String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    try (SessionDataSet dataSetWrapper = session.executeQueryStatement(sql, 60_000)) {
      long count = 0;
      while (dataSetWrapper.hasNext()) {
        RowRecord record = dataSetWrapper.next();
        Field field = record.getFields().get(0);
        switch (field.getDataType()) {
          case INT32 -> count = field.getIntV();
          case INT64 -> count = field.getLongV();
          case BOOLEAN,
              FLOAT,
              DOUBLE,
              TEXT,
              VECTOR,
              UNKNOWN,
              TIMESTAMP,
              DATE,
              BLOB,
              STRING,
              OBJECT ->
              throw new AssertionError("Unexpected count type: " + field.getDataType());
        }
      }
      return count;
    }
  }

  // Mix non-null and null typed/string records; only three non-null s1 values should remain.
  @Test
  public void insertRecordNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      String deviceId = database + ".clsu.d1";
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

  // Mix aligned typed/string records with nulls; eventually count exactly three s1 values.
  @Test
  public void insertAlignedRecordNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      String deviceId = database + ".clsu.aligned_d1";
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
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(Duration.ZERO)
          .pollInterval(Duration.ofMillis(100))
          .atMost(Duration.ofSeconds(30))
          .ignoreExceptionsMatching(e -> e instanceof IoTDBConnectionException)
          .untilAsserted(
              () -> {
                long nums = queryCountRecords(session, "select count(s1) from " + deviceId);
                assertEquals(3, nums);
              });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // Batch typed/string records with nulls and verify each device retains three values.
  @Test
  public void insertRecordsNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      String deviceId1 = database + ".clsu.d2";
      String deviceId2 = database + ".clsu.d3";
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

      Awaitility.await()
          .pollInSameThread()
          .pollDelay(Duration.ZERO)
          .pollInterval(Duration.ofMillis(100))
          .atMost(Duration.ofSeconds(30))
          .ignoreExceptionsMatching(e -> e instanceof IoTDBConnectionException)
          .untilAsserted(
              () -> {
                long nums = queryCountRecords(session, "select count(s1) from " + deviceId1);
                assertEquals(3, nums);
                nums = queryCountRecords(session, "select count(s2) from " + deviceId2);
                assertEquals(3, nums);
              });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // Batch aligned records with nulls and verify the expected per-device column counts.
  @Test
  public void insertAlignedRecordsNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);
      String deviceId1 = database + ".clsu.aligned_d2";
      String deviceId2 = database + ".clsu.aligned_d3";
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

  // Mix typed/string batches for one device; nulls must leave exactly six s1 values.
  @Test
  public void insertRecordsOfOneDeviceNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);
      String deviceId1 = database + ".clsu.InsertRecordsOfOneDevice";
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

  // Mix aligned batches for one device; nulls must leave exactly six s1 values.
  @Test
  public void insertAlignedRecordsOfOneDeviceNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);
      String deviceId1 = database + ".clsu.InsertAlignedRecordsOfOneDevice";
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

  // Insert an all-null tablet; s1 must stay empty before and after flush.
  @Test
  public void insertTabletNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      String deviceId = database + ".clsu.d1";
      Tablet tablet =
          new Tablet(
              deviceId,
              Arrays.asList(
                  new MeasurementSchema("s1", TSDataType.BOOLEAN),
                  new MeasurementSchema("s2", TSDataType.INT32)),
              3);
      tablet.addTimestamp(0, 300);
      tablet.addValue("s1", 0, null);
      tablet.addValue("s2", 0, null);
      tablet.addTimestamp(1, 400);
      tablet.addValue("s1", 1, null);
      tablet.addValue("s2", 1, null);
      tablet.addTimestamp(2, 500);
      tablet.addValue("s1", 2, null);
      tablet.addValue("s2", 2, null);
      session.insertTablet(tablet);
      long nums = queryCountRecords(session, "select count(s1) from " + deviceId);
      assertEquals(0, nums);
      session.executeNonQueryStatement("flush");
      nums = queryCountRecords(session, "select count(s1) from " + deviceId);
      assertEquals(0, nums);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // Flush an all-null aligned tablet; counts stay zero and no broken TsFile is produced.
  @Test
  public void insertAlignedTabletNullTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      String deviceId = database + ".clsu.aligned_d1";
      Tablet tablet =
          new Tablet(
              deviceId,
              Arrays.asList(
                  new MeasurementSchema("s1", TSDataType.BOOLEAN),
                  new MeasurementSchema("s2", TSDataType.INT32)),
              3);
      tablet.addTimestamp(0, 300);
      tablet.addValue("s1", 0, null);
      tablet.addValue("s2", 0, null);
      tablet.addTimestamp(1, 400);
      tablet.addValue("s1", 1, null);
      tablet.addValue("s2", 1, null);
      tablet.addTimestamp(2, 500);
      tablet.addValue("s1", 2, null);
      tablet.addValue("s2", 2, null);
      session.insertAlignedTablet(tablet);
      long nums = queryCountRecords(session, "select count(s1) from " + deviceId);
      assertEquals(0, nums);
      session.executeNonQueryStatement("flush");
      nums = queryCountRecords(session, "select count(s1) from " + deviceId);
      assertEquals(0, nums);
      for (DataNodeWrapper dn : EnvFactory.getEnv().getDataNodeWrapperList()) {
        Path databaseDir = Paths.get(dn.getDataDir(), "datanode", "data", "sequence", database);
        if (Files.isDirectory(databaseDir)) {
          // A shared cluster assigns different region IDs; inspect every region of this database.
          try (Stream<Path> files = Files.walk(databaseDir)) {
            assertFalse(files.anyMatch(file -> file.getFileName().toString().endsWith("broken")));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // Reject aligned tablets containing a null measurement name for both value setters.
  @Test
  public void insertTabletNullMeasurementTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = database + ".clsu.aligned_d1";
      Tablet tablet =
          new Tablet(
              deviceId,
              Arrays.asList(
                  new MeasurementSchema("s1", TSDataType.BOOLEAN),
                  new MeasurementSchema(null, TSDataType.INT32)),
              1);
      tablet.addTimestamp(0, 300);
      tablet.addValue("s1", 0, true);
      tablet.addValue(null, 0, 1);
      session.insertAlignedTablet(tablet);
      fail();
    } catch (Exception e) {
      Assert.assertEquals("measurement should be non null value", e.getMessage());
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = database + ".clsu.aligned_d1";
      Tablet tablet =
          new Tablet(
              deviceId,
              Arrays.asList(
                  new MeasurementSchema("s1", TSDataType.BOOLEAN),
                  new MeasurementSchema(null, TSDataType.INT32)),
              1);
      tablet.addTimestamp(0, 300);
      tablet.addValue(0, 0, true);
      tablet.addValue(0, 1, 1);
      session.insertAlignedTablet(tablet);
      fail();
    } catch (Exception e) {
      Assert.assertEquals("measurement should be non null value", e.getMessage());
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = database + ".clsu.aligned_d1";
      Tablet tablet =
          new Tablet(
              deviceId,
              Arrays.asList(
                  new MeasurementSchema("s1", TSDataType.BOOLEAN),
                  new MeasurementSchema(null, TSDataType.INT32)),
              1);
      tablet.addTimestamp(0, 300);
      tablet.addValue("s1", 0, true);
      // doesn't insert 2nd measurement
      session.insertAlignedTablet(tablet);
      fail();
    } catch (Exception e) {
      Assert.assertEquals("measurement should be non null value", e.getMessage());
    }
  }
}
