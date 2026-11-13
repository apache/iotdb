/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.it.utils.DeletionTestUtils;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBRecreatedTimeseriesCompactionIT {

  private static final String DATABASE = "root.repro";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.simple.SimpleConsensus")
        .setDataReplicationFactor(1)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setInnerCompactionCandidateFileNum(2);
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setCompactionScheduleInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void resetCompactionConfiguration() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION 'enable_seq_space_compaction'='false'");
      statement.execute("SET CONFIGURATION 'enable_unseq_space_compaction'='false'");
      statement.execute("SET CONFIGURATION 'enable_cross_space_compaction'='false'");
    }
  }

  @Test
  public void testDeletionAfterRecreatingTwoLevelTimeseriesSurvivesCompaction() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("CREATE TIMESERIES root.repro.s1 WITH DATATYPE=INT64");
      statement.execute("INSERT INTO root.repro(time, s1) VALUES (1, 1)");
      statement.execute("FLUSH");

      statement.execute("DELETE TIMESERIES root.repro.s1");
      statement.execute("CREATE TIMESERIES root.repro.s1 WITH DATATYPE=INT64");
      statement.execute("INSERT INTO root.repro(time, s1) VALUES (2, 2)");
      statement.execute("FLUSH");

      assertOnlyRecreatedTimeseriesDataExists(statement);
      Assert.assertEquals(2, countSequenceTsFiles());

      statement.execute("SET CONFIGURATION 'enable_seq_space_compaction'='true'");

      Awaitility.await()
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .atMost(30, TimeUnit.SECONDS)
          .until(() -> countSequenceTsFiles() == 1);

      assertOnlyRecreatedTimeseriesDataExists(statement);
    }
  }

  @Test
  public void testDeletionAndRewriteSurviveSeqUnseqCompaction() throws Exception {
    final String database = "root.delete_compaction";
    List<Path> sourceFiles = new ArrayList<>();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
      statement.execute("CREATE TIMESERIES " + database + ".d1.s1 WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES " + database + ".d1.s2 WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES " + database + ".d2.s1 WITH DATATYPE=INT64");

      // Keep both the unrewritten deleted point and a surviving measurement in the same files.
      statement.execute(
          "INSERT INTO " + database + ".d1(time,s1,s2) VALUES(1,1,101),(2,2,102),(3,3,103)");
      statement.execute("INSERT INTO " + database + ".d2(time,s1) VALUES(1,201),(2,202),(3,203)");
      statement.execute("FLUSH");
      List<Path> sequenceFiles = DeletionTestUtils.getClosedTsFiles(database, true);
      Assert.assertEquals(1, sequenceFiles.size());
      Assert.assertEquals(
          Map.of(1L, 1L, 2L, 2L, 3L, 3L),
          DeletionTestUtils.readTreePoints(sequenceFiles.get(0), database + ".d1", "s1"));

      statement.execute("INSERT INTO " + database + ".d1(time,s1) VALUES(1,10),(2,20)");
      statement.execute("INSERT INTO " + database + ".d2(time,s1) VALUES(2,220)");
      statement.execute("FLUSH");
      List<Path> unsequenceFiles = DeletionTestUtils.getClosedTsFiles(database, false);
      Assert.assertEquals(1, unsequenceFiles.size());
      Assert.assertEquals(
          Map.of(1L, 10L, 2L, 20L),
          DeletionTestUtils.readTreePoints(unsequenceFiles.get(0), database + ".d1", "s1"));
      sourceFiles.addAll(sequenceFiles);
      sourceFiles.addAll(unsequenceFiles);

      statement.execute("DELETE FROM " + database + ".d1.s1 WHERE time <= 2");
      statement.execute("INSERT INTO " + database + ".d1(time,s1) VALUES(1,100)");
      statement.execute("FLUSH");
      assertCompactionData(statement);
      sourceFiles.addAll(DeletionTestUtils.getClosedTsFiles(database, false));

      // Only cross-space compaction is enabled. Track the actual input paths and verify the
      // output's cross-compaction generation, rather than inferring a task from a file count.
      // These tiny level-0 unseq files must be eligible without an inner-space compaction first.
      statement.execute("SET CONFIGURATION 'min_cross_compaction_unseq_file_level'='0'");
      statement.execute("SET CONFIGURATION 'enable_cross_space_compaction'='true'");
    }

    Awaitility.await()
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .until(
            () -> {
              if (sourceFiles.stream().anyMatch(Files::exists)
                  || !DeletionTestUtils.getClosedTsFiles(database, false).isEmpty()) {
                return false;
              }
              for (Path file : DeletionTestUtils.getClosedTsFiles(database, true)) {
                if (TsFileNameGenerator.getTsFileName(file.getFileName().toString())
                            .getCrossCompactionCnt()
                        > 0
                    && Long.valueOf(100)
                        .equals(
                            DeletionTestUtils.readTreePoints(file, database + ".d1", "s1")
                                .get(1L))) {
                  return true;
                }
              }
              return false;
            });

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      assertCompactionData(statement);
    }
    TestUtils.stopForciblyAndRestartDataNodes();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      assertCompactionData(statement);
      statement.execute("INSERT INTO " + database + ".d1(time,s1,s2) VALUES(4,4,104)");
      statement.execute("DELETE FROM " + database + ".d1.s1 WHERE time = 3");
      statement.execute("FLUSH");
      assertCompactionRows(
          statement, new String[] {"1,100,101,", "2,null,102,", "3,null,103,", "4,4,104,"});
    }
  }

  private static void assertCompactionData(Statement statement) throws Exception {
    assertCompactionRows(statement, new String[] {"1,100,101,", "2,null,102,", "3,3,103,"});
  }

  @Test
  public void testDeletionQueryCancellationAllowsCompactionAndContinuedWrites() throws Exception {
    final String database = "root.delete_cancel";
    final String device = database + ".d1";
    final String query = "SELECT s1,s2 FROM " + device + " ORDER BY time";
    // As in IoTDBShowQueriesIT, leave a full TsBlock after the initial Session fetch so the
    // query stays registered until it is explicitly killed. Do not assert a concurrent snapshot.
    final int rowCount = SessionConfig.DEFAULT_FETCH_SIZE + 1000;
    try (ISession session = EnvFactory.getEnv().getSessionConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
      statement.execute("CREATE TIMESERIES " + device + ".s1 WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES " + device + ".s2 WITH DATATYPE=INT64");
      Tablet tablet =
          new Tablet(
              device,
              List.of(
                  new MeasurementSchema("s1", TSDataType.INT64),
                  new MeasurementSchema("s2", TSDataType.INT64)),
              rowCount);
      for (int time = 0; time < rowCount; time++) {
        tablet.addTimestamp(time, time);
        tablet.addValue("s1", time, 100000L + time);
        tablet.addValue("s2", time, 200000L + time);
      }
      session.insertTablet(tablet);
      statement.execute("FLUSH");
      statement.execute(
          "INSERT INTO "
              + device
              + "(time,s1,s2) VALUES("
              + rowCount
              + ","
              + (100000L + rowCount)
              + ","
              + (200000L + rowCount)
              + ")");
      statement.execute("FLUSH");
      final List<Path> inputs = DeletionTestUtils.getClosedTsFiles(database, true);
      Assert.assertEquals(2, inputs.size());

      try (SessionDataSet pending = session.executeQueryStatement(query)) {
        Assert.assertTrue(pending.hasNext());
        final String queryId = findDeletionQueryId(statement, query);
        Assert.assertNotNull("The unconsumed query must actually be registered", queryId);
        for (int time = 0; time < 5; time++) {
          statement.execute("DELETE FROM " + device + ".s1 WHERE time=" + time);
        }
        statement.execute("KILL QUERY '" + queryId + "'");
        Awaitility.await()
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .atMost(30, TimeUnit.SECONDS)
            .until(() -> findDeletionQueryId(statement, query) == null);
      }
      assertDeletionCancellationRows(statement, device, rowCount, false);
      statement.execute("SET CONFIGURATION 'enable_seq_space_compaction'='true'");
      try {
        Awaitility.await()
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .atMost(60, TimeUnit.SECONDS)
            .until(
                () -> {
                  if (inputs.stream().anyMatch(Files::exists)) {
                    return false;
                  }
                  for (Path output : DeletionTestUtils.getClosedTsFiles(database, true)) {
                    if (TsFileNameGenerator.getTsFileName(output.getFileName().toString())
                            .getInnerCompactionCnt()
                        > 0) {
                      return true;
                    }
                  }
                  return false;
                });
        assertDeletionCancellationRows(statement, device, rowCount, false);
        // Keep the original Session after cancellation: it must still complete writes/deletes.
        session.executeNonQueryStatement("INSERT INTO " + device + "(time,s1) VALUES(2,902)");
        session.deleteData(List.of(device + ".s1"), 7, 7);
        session.executeNonQueryStatement("FLUSH");
        assertDeletionCancellationRows(statement, device, rowCount, true);
        Assert.assertNull(findDeletionQueryId(statement, query));
      } finally {
        statement.execute("SET CONFIGURATION 'enable_seq_space_compaction'='false'");
      }
    }
  }

  private static String findDeletionQueryId(Statement statement, String sql) throws Exception {
    try (ResultSet queries = statement.executeQuery("SHOW QUERIES")) {
      while (queries.next()) {
        if (sql.equals(queries.getString("Statement"))) {
          return queries.getString("QueryId");
        }
      }
      return null;
    }
  }

  private static void assertDeletionCancellationRows(
      Statement statement, String device, int lastTime, boolean continued) throws Exception {
    try (ResultSet rows =
        statement.executeQuery("SELECT s1,s2 FROM " + device + " ORDER BY time")) {
      for (int time = 0; time <= lastTime; time++) {
        Assert.assertTrue("Missing timestamp " + time, rows.next());
        Assert.assertEquals(time, rows.getLong(1));
        if (continued && time == 2) {
          Assert.assertEquals(902, rows.getLong(2));
        } else if (time < 5 || (continued && time == 7)) {
          Assert.assertNull(rows.getObject(2));
        } else {
          Assert.assertEquals(100000L + time, rows.getLong(2));
        }
        Assert.assertEquals(200000L + time, rows.getLong(3));
      }
      Assert.assertFalse(rows.next());
    }
  }

  private static void assertCompactionRows(Statement statement, String[] expected)
      throws Exception {
    try (ResultSet rows =
        statement.executeQuery("SELECT s1,s2 FROM root.delete_compaction.d1 ORDER BY time")) {
      TestUtils.assertResultSetEqual(
          rows, "Time,root.delete_compaction.d1.s1,root.delete_compaction.d1.s2,", expected);
    }
    try (ResultSet rows =
        statement.executeQuery("SELECT s1 FROM root.delete_compaction.d2 ORDER BY time")) {
      TestUtils.assertResultSetEqual(
          rows, "Time,root.delete_compaction.d2.s1,", new String[] {"1,201,", "2,220,", "3,203,"});
    }
  }

  private static void assertOnlyRecreatedTimeseriesDataExists(Statement statement)
      throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.repro")) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(2, resultSet.getLong("Time"));
      Assert.assertEquals(2, resultSet.getLong("root.repro.s1"));
      Assert.assertFalse(resultSet.next());
    }
  }

  private static long countSequenceTsFiles() throws IOException {
    Path databaseSequenceDirectory =
        Paths.get(
            EnvFactory.getEnv().getDataNodeWrapper(0).getDataPath(),
            IoTDBConstant.SEQUENCE_FOLDER_NAME,
            DATABASE);
    if (!Files.exists(databaseSequenceDirectory)) {
      return 0;
    }
    try (Stream<Path> paths = Files.walk(databaseSequenceDirectory)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(TSFILE_SUFFIX))
          .count();
    }
  }
}
