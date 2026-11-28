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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(LocalStandaloneIT.class)
public class IoTDBWriteBackpressureIT {
  @BeforeClass
  public static void setUp() {
    // A distinct fixture is necessary: these budgets deliberately reject a bounded 2 MiB write.
    EnvFactory.getEnv().getConfig().getDataNodeJVMConfig().setMaxHeapSize(512);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setWalMode("SYNC")
        .setEnableMemControl(true)
        .setEnableLastCache(false)
        .setDatanodeMemoryProportion("1:1:1:1:1:1")
        .setWriteMemoryProportion("1:99")
        .setMemtableSizeThreshold(64L * 1024 * 1024);
    Properties properties = new Properties();
    properties.setProperty("write_memory_variation_report_proportion", "0");
    properties.setProperty("max_waiting_time_when_insert_blocked", "1000");
    properties.setProperty("check_period_when_insert_blocked", "10");
    properties.setProperty("wal_entry_size_limit_in_byte", "16777216");
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
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test(timeout = 120_000)
  public void testMemoryRejectionRollsBackAndOriginalWritersResume() throws Exception {
    try (Session writer = newWriter();
        Session other = newWriter()) {
      writer.executeNonQueryStatement("CREATE DATABASE root.backpressure");
      writer.executeNonQueryStatement("CREATE DATABASE root.backpressure_rejected");
      writer.executeNonQueryStatement("CREATE TIMESERIES root.backpressure.plain.s TEXT");
      writer.executeNonQueryStatement(
          "CREATE ALIGNED TIMESERIES root.backpressure.aligned(s TEXT)");
      writer.executeNonQueryStatement("CREATE TIMESERIES root.backpressure_rejected.plain.s TEXT");
      writer.executeNonQueryStatement(
          "CREATE ALIGNED TIMESERIES root.backpressure_rejected.aligned(s TEXT)");
      writer.executeNonQueryStatement(
          "INSERT INTO root.backpressure.plain(time,s) VALUES(0,'before')");
      other.executeNonQueryStatement(
          "INSERT INTO root.backpressure.aligned(time,s) ALIGNED VALUES(0,'before')");
      long baseline = memoryUsage();
      assertTrue(baseline > 0);
      for (boolean aligned : new boolean[] {false, true}) {
        // Keep the target processor empty: it cannot escape admission by selecting its own
        // existing memtable for flush. Other Regions can flush, but not the pending reservation.
        String device = "root.backpressure_rejected." + (aligned ? "aligned" : "plain");
        Tablet tablet =
            new Tablet(
                device, Collections.singletonList(new MeasurementSchema("s", TSDataType.TEXT)), 64);
        for (int row = 0; row < 64; row++) {
          tablet.addTimestamp(row, row + 1);
          tablet.addValue("s", row, "x".repeat(32 * 1024));
        }
        StatementExecutionException rejected =
            assertThrows(
                StatementExecutionException.class,
                () -> {
                  if (aligned) {
                    other.insertAlignedTablet(tablet);
                  } else {
                    writer.insertTablet(tablet);
                  }
                });
        assertEquals(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(), rejected.getStatusCode());
        // A single tablet/processor times out during admission, before WAL or any row insertion.
        await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  assertTrue("Rejected reservation leaked memory", memoryUsage() <= baseline);
                  assertEquals(0, memoryUsage("root.backpressure_rejected"));
                });
        try (Connection connection = EnvFactory.getEnv().getConnection();
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery("SELECT s FROM " + device)) {
          assertFalse("Rejected tablet must not insert any rows", result.next());
        }
      }
      // Keep both original clients. Clearing admission pressure must release every writer.
      writer.executeNonQueryStatement(
          "INSERT INTO root.backpressure.plain(time,s) VALUES(1,'after')");
      other.executeNonQueryStatement(
          "INSERT INTO root.backpressure.aligned(time,s) ALIGNED VALUES(1,'after')");
      writer.executeNonQueryStatement(
          "INSERT INTO root.backpressure_rejected.plain(time,s) VALUES(1,'recovered')");
      other.executeNonQueryStatement(
          "INSERT INTO root.backpressure_rejected.aligned(time,s) ALIGNED VALUES(1,'recovered')");
      assertTrue(
          "Recovered writes must be charged to their database",
          memoryUsage("root.backpressure_rejected") > 0);
      writer.executeNonQueryStatement("FLUSH");
      await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                assertEquals(0, memoryUsage());
                assertEquals(0, memoryUsage("root.backpressure_rejected"));
              });
      assertRows("root.backpressure.plain", true);
      assertRows("root.backpressure.aligned", true);
      for (String device : new String[] {"plain", "aligned"}) {
        try (Connection connection = EnvFactory.getEnv().getConnection();
            Statement statement = connection.createStatement();
            ResultSet result =
                statement.executeQuery("SELECT s FROM root.backpressure_rejected." + device)) {
          assertTrue(result.next());
          assertEquals(1, result.getLong(1));
          assertEquals("recovered", result.getString(2));
          assertFalse(result.next());
        }
      }
      // A second admission after flush also checks that rejection did not permanently latch.
      other.executeNonQueryStatement(
          "INSERT INTO root.backpressure.aligned(time,s) ALIGNED VALUES(1,'after')");
      writer.executeNonQueryStatement("FLUSH");
      await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                assertEquals(0, memoryUsage());
                assertEquals(0, memoryUsage("root.backpressure_rejected"));
              });
    }
  }

  private static Session newWriter() throws Exception {
    Session session =
        new Session.Builder()
            .host(EnvFactory.getEnv().getDataNodeWrapper(0).getIp())
            .port(EnvFactory.getEnv().getDataNodeWrapper(0).getPort())
            .enableAutoFetch(false)
            .maxRetryCount(0)
            .build();
    // Expose the memory rejection rather than hiding it behind the SDK's default 60 retries.
    session.open(false, 30_000);
    return session;
  }

  private static void assertRows(String device, boolean after) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT s FROM " + device)) {
      assertTrue(result.next());
      assertEquals(0, result.getLong(1));
      assertEquals("before", result.getString(2));
      if (after) {
        assertTrue(result.next());
        assertEquals(1, result.getLong(1));
        assertEquals("after", result.getString(2));
      }
      assertFalse("Rejected tablet must not insert any rows", result.next());
    }
  }

  private static long memoryUsage() {
    return memoryUsage("root.backpressure");
  }

  private static long memoryUsage(String database) {
    return EnvFactory.getEnv().getMetricPrometheusReporterContents(null).stream()
        .flatMap(String::lines)
        .filter(
            line -> line.startsWith("mem{") && line.contains("name=\"database_" + database + "\""))
        .mapToLong(line -> (long) Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing database write memory gauge"));
  }
}
