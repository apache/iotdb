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

package org.apache.iotdb.db.it;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(LocalStandaloneIT.class)
public class IoTDBWriteMemoryLifecycleIT {
  private static final String DATABASE = "root.write_memory";
  private static final String[] DEVICES = {DATABASE + ".plain", DATABASE + ".aligned"};
  private static final String OVERSIZED_TEXT = "x".repeat(32 * 1024);
  private final Map<Long, String> expected = new TreeMap<>();

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.simple.SimpleConsensus")
        .setWalMode("SYNC")
        .setEnableMemControl(true)
        .setEnableLastCache(false)
        .setPrimitiveArraySize(64)
        .setTimePartitionInterval(1000)
        .setMemtableSizeThreshold(64L * 1024 * 1024)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    // Keep flushes deterministic and reject WAL entries only after memory admission.
    Properties properties = new Properties();
    properties.setProperty("enable_timed_flush_seq_memtable", "false");
    properties.setProperty("enable_timed_flush_unseq_memtable", "false");
    properties.setProperty("wal_entry_size_limit_in_byte", "16384");
    // Read live accounting, not the reporter's 15-second cached snapshot.
    properties.setProperty("prometheus_reporter_async_update", "false");
    ((MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(properties);
    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    EnvFactory.getEnv().initClusterEnvironment();
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      session.executeNonQueryStatement("CREATE TIMESERIES " + DEVICES[0] + ".n INT64");
      session.executeNonQueryStatement("CREATE TIMESERIES " + DEVICES[0] + ".t TEXT");
      session.executeNonQueryStatement(
          "CREATE ALIGNED TIMESERIES " + DEVICES[1] + "(n INT64, t TEXT)");
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testArrayBoundariesSparseColumnsAndRepeatedFlush() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // Exercise 63 -> 64 -> 65 and multiple array allocations, for both TVList types.
      insert(session, 0, 63);
      insert(session, 63, 1);
      insert(session, 64, 1);
      insert(session, 65, 64);
      long beforeNewColumn = memoryUsage();
      session.insertAlignedRecord(
          DEVICES[1],
          128,
          Collections.singletonList("late"),
          Collections.singletonList(TSDataType.TEXT),
          Collections.singletonList("new-column"));
      assertTrue("A late aligned column must be charged", memoryUsage() > beforeNewColumn);
      assertData();
      flushAndAssertReleased(session);
      for (int cycle = 1; cycle <= 3; cycle++) {
        insert(session, cycle * 129, 129);
        // This timestamp is already on disk: exercise unsequence processors and overwrite merge.
        overwrite(session, 0, "replacement-" + cycle);
        assertTrue("Live writes must be charged after reusing pooled arrays", memoryUsage() > 0);
        assertData();
        flushAndAssertReleased(session);
        assertData();
      }
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT late FROM " + DEVICES[1])) {
      assertTrue(result.next());
      assertEquals(128, result.getLong(1));
      assertEquals("new-column", result.getString(2));
      assertFalse(result.next());
    }
  }

  @Test
  public void testWalRejectionRollsBackAndAllowsFurtherWrites() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insert(session, 0, 63);
      long baseline = memoryUsage();
      assertTrue(baseline > 0);
      for (int attempt = 0; attempt < 3; attempt++) {
        for (int device = 0; device < DEVICES.length; device++) {
          final boolean aligned = device == 1;
          final String path = DEVICES[device];
          // A new measurement exercises metadata rollback as well as array and Binary rollback.
          StatementExecutionException failure =
              assertThrows(
                  StatementExecutionException.class,
                  () -> {
                    if (aligned) {
                      session.insertAlignedRecord(
                          path,
                          63,
                          Collections.singletonList("rejected"),
                          Collections.singletonList(TSDataType.TEXT),
                          Collections.singletonList(OVERSIZED_TEXT));
                    } else {
                      session.insertRecord(
                          path,
                          63,
                          Collections.singletonList("rejected"),
                          Collections.singletonList(TSDataType.TEXT),
                          Collections.singletonList(OVERSIZED_TEXT));
                    }
                  });
          assertEquals(TSStatusCode.WAL_ENTRY_TOO_LARGE.getStatusCode(), failure.getStatusCode());
          assertEquals("Rejected row leaked memory", baseline, memoryUsage());
          Tablet tablet = tablet(path, 63, 2, OVERSIZED_TEXT);
          StatementExecutionException batchFailure =
              assertThrows(
                  StatementExecutionException.class,
                  () -> {
                    if (aligned) {
                      session.insertAlignedTablet(tablet);
                    } else {
                      session.insertTablet(tablet);
                    }
                  });
          assertEquals(
              TSStatusCode.WAL_ENTRY_TOO_LARGE.getStatusCode(), batchFailure.getStatusCode());
          assertEquals("Rejected tablet leaked memory", baseline, memoryUsage());
        }
      }
      assertData();
      insert(session, 63, 66);
      assertData();
      flushAndAssertReleased(session);
      assertData();
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String device : DEVICES) {
        try (ResultSet result = statement.executeQuery("SELECT rejected FROM " + device)) {
          assertFalse("Rejected WAL entry must not insert data", result.next());
        }
      }
    }
  }

  @Test
  public void testCrashRecoveryThenWriteAndRelease() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insert(session, 0, 129);
      assertTrue(memoryUsage() > 0);
    }
    // No FLUSH and no graceful shutdown: acknowledged data must come back through WAL recovery.
    TestUtils.stopForciblyAndRestartDataNodes();
    assertData();
    // Recovery flushes the replayed memtable and seals its TsFile before accepting requests.
    assertEquals("Recovery must leave no live write memory", 0, memoryUsage());
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insert(session, 129, 129);
      overwrite(session, 0, "after-recovery");
      assertTrue("New writes after recovery must be charged", memoryUsage() > 0);
      assertData();
      flushAndAssertReleased(session);
    }
    TestUtils.stopForciblyAndRestartDataNodes();
    assertData();
    assertEquals("Closed files must not retain write memory after restart", 0, memoryUsage());
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insert(session, 258, 65);
      assertTrue(memoryUsage() > 0);
      flushAndAssertReleased(session);
      assertData();
    }
  }

  @Test
  public void testTableTabletSplitAcrossDevicesAndTimePartitions() throws Exception {
    String database = "write_memory_table";
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
      statement.execute("USE " + database);
      statement.execute("CREATE TABLE readings(device STRING TAG, n INT64 FIELD, t STRING FIELD)");
      // Each request contains two devices and spans the 1000 ms partition boundary.
      for (int cycle = 0; cycle < 2; cycle++) {
        StringBuilder sql = new StringBuilder("INSERT INTO readings(time,device,n,t) VALUES ");
        for (int row = 0; row < 130; row++) {
          for (int device = 0; device < 2; device++) {
            if (row != 0 || device != 0) {
              sql.append(',');
            }
            sql.append('(')
                .append(935 + row)
                .append(", 'd")
                .append(device)
                .append("', ")
                .append(row + cycle * 1000)
                .append(", ")
                .append(row % 3 == 0 ? "null" : "'value-" + cycle + "-" + row + "'")
                .append(')');
          }
        }
        statement.execute(sql.toString());
        assertTrue("All split processors must account for live writes", memoryUsage(database) > 0);
        // The second cycle overwrites flushed data, exercising unsequence ranges in both
        // partitions.
        for (int phase = 0; phase < 2; phase++) {
          try (ResultSet result =
              statement.executeQuery("SELECT time,device,n,t FROM readings ORDER BY time,device")) {
            for (int row = 0; row < 130; row++) {
              for (int device = 0; device < 2; device++) {
                assertTrue(result.next());
                assertEquals(935 + row, result.getLong(1));
                assertEquals("d" + device, result.getString(2));
                assertEquals(row + cycle * 1000, result.getLong(3));
                assertEquals(
                    row % 3 == 0 ? null : "value-" + cycle + "-" + row, result.getString(4));
              }
            }
            assertFalse(result.next());
          }
          if (phase == 0) {
            statement.execute("FLUSH");
            await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(0, memoryUsage(database)));
          }
        }
      }
    }
  }

  private void insert(ISession session, int start, int count) throws Exception {
    session.insertTablet(tablet(DEVICES[0], start, count, null));
    session.insertAlignedTablet(tablet(DEVICES[1], start, count, null));
    for (int time = start; time < start + count; time++) {
      expected.put((long) time, text(time));
    }
  }

  private static Tablet tablet(String device, int start, int count, String override) {
    Tablet tablet =
        new Tablet(
            device,
            Arrays.asList(
                new MeasurementSchema("n", TSDataType.INT64),
                new MeasurementSchema("t", TSDataType.TEXT)),
            count);
    for (int row = 0; row < count; row++) {
      int time = start + row;
      tablet.addTimestamp(row, time);
      tablet.addValue("n", row, (long) time);
      tablet.addValue("t", row, override == null ? text(time) : override);
    }
    return tablet;
  }

  private static String text(int time) {
    return time % 3 == 0 ? null : "value-" + time;
  }

  private void overwrite(ISession session, long time, String value) throws Exception {
    session.insertRecord(
        DEVICES[0],
        time,
        Collections.singletonList("t"),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList(value));
    session.insertAlignedRecord(
        DEVICES[1],
        time,
        Collections.singletonList("t"),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList(value));
    expected.put(time, value);
  }

  private void assertData() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String device : DEVICES) {
        try (ResultSet result = statement.executeQuery("SELECT n,t FROM " + device)) {
          for (Map.Entry<Long, String> row : expected.entrySet()) {
            assertTrue("Missing timestamp " + row.getKey() + " in " + device, result.next());
            assertEquals((long) row.getKey(), result.getLong(1));
            assertEquals((long) row.getKey(), result.getLong(2));
            assertFalse(result.wasNull());
            assertEquals(row.getValue(), result.getString(3));
          }
          assertFalse("Unexpected extra row in " + device, result.next());
        }
      }
    }
  }

  private static void flushAndAssertReleased(ISession session) throws Exception {
    session.executeNonQueryStatement("FLUSH");
    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(0, memoryUsage()));
  }

  private static long memoryUsage() {
    return memoryUsage(DATABASE);
  }

  private static long memoryUsage(String database) {
    String label = "name=\"database_" + database + "\"";
    return EnvFactory.getEnv().getMetricPrometheusReporterContents(null).stream()
        .flatMap(content -> content.lines())
        .filter(line -> line.startsWith("mem{") && line.contains(label))
        .mapToLong(line -> (long) Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing database memory gauge: " + label));
  }
}
