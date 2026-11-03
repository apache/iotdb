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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.it;

import org.apache.iotdb.db.it.utils.DeletionTestUtils;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(LocalStandaloneIT.class)
public class IoTDBDeletionRecoveryIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setWalMode("SYNC")
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.simple.SimpleConsensus")
        .setDataReplicationFactor(1)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setMemtableSizeThreshold(16 * 1024 * 1024)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDeleteAndRewriteSurviveForcedRestart() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.restart_delete");
      statement.execute("CREATE TIMESERIES root.restart_delete.d1.s1 WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES root.restart_delete.d1.s2 WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES root.restart_delete.d2.s1 WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES root.restart_delete.d2.s2 WITH DATATYPE=INT64");

      statement.execute("INSERT INTO root.restart_delete.d1(time,s1,s2) VALUES(1,11,101)");
      statement.execute("INSERT INTO root.restart_delete.d1(time,s1) VALUES(2,12)");
      statement.execute("INSERT INTO root.restart_delete.d2(time,s1,s2) VALUES(2,22,202)");
      statement.execute("FLUSH");
      Path history =
          DeletionTestUtils.assertSharedTsFile(
              "root.restart_delete",
              new PlainDeviceID("root.restart_delete.d1"),
              new PlainDeviceID("root.restart_delete.d2"));
      assertEquals(
          Map.of(1L, 11L, 2L, 12L),
          DeletionTestUtils.readTreePoints(history, "root.restart_delete.d1", "s1"));
      List<Path> closedFiles = DeletionTestUtils.getClosedTsFiles("root.restart_delete", true);

      statement.execute("INSERT INTO root.restart_delete.d1(time,s1) VALUES(1,111)");
      statement.execute("INSERT INTO root.restart_delete.d1(time,s1) VALUES(3,13)");
      statement.execute("INSERT INTO root.restart_delete.d1(time,s1,s2) VALUES(4,14,104)");
      assertEquals(closedFiles, DeletionTestUtils.getClosedTsFiles("root.restart_delete", true));
      assertTrue(DeletionTestUtils.getClosedTsFiles("root.restart_delete", false).isEmpty());
      assertEquals(
          Map.of(1L, 11L, 2L, 12L),
          DeletionTestUtils.readTreePoints(history, "root.restart_delete.d1", "s1"));

      statement.execute("DELETE FROM root.restart_delete.d1.s1 WHERE time <= 3");
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1,s2 FROM root.restart_delete.d1 ORDER BY time")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertNull(resultSet.getObject(2));
        assertEquals(101, resultSet.getLong(3));
        assertTrue(resultSet.next());
        assertEquals(4, resultSet.getLong(1));
        assertEquals(14, resultSet.getLong(2));
        assertEquals(104, resultSet.getLong(3));
        assertFalse(resultSet.next());
      }

      statement.execute("INSERT INTO root.restart_delete.d1(time,s1) VALUES(2,1202)");
      assertEquals(closedFiles, DeletionTestUtils.getClosedTsFiles("root.restart_delete", true));
      assertTrue(DeletionTestUtils.getClosedTsFiles("root.restart_delete", false).isEmpty());
      try (ResultSet rows =
          statement.executeQuery("SELECT s1,s2 FROM root.restart_delete.d1 ORDER BY time")) {
        TestUtils.assertResultSetEqual(
            rows,
            "Time,root.restart_delete.d1.s1,root.restart_delete.d1.s2,",
            new String[] {"1,null,101,", "2,1202,null,", "4,14,104,"});
      }
    }

    TestUtils.stopForciblyAndRestartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1,s2 FROM root.restart_delete.d1 ORDER BY time")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertNull(resultSet.getObject(2));
        assertEquals(101, resultSet.getLong(3));
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getLong(1));
        assertEquals(1202, resultSet.getLong(2));
        assertNull(resultSet.getObject(3));
        assertTrue(resultSet.next());
        assertEquals(4, resultSet.getLong(1));
        assertEquals(14, resultSet.getLong(2));
        assertEquals(104, resultSet.getLong(3));
        assertFalse(resultSet.next());
      }
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1,s2 FROM root.restart_delete.d2 ORDER BY time")) {
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getLong(1));
        assertEquals(22, resultSet.getLong(2));
        assertEquals(202, resultSet.getLong(3));
        assertFalse(resultSet.next());
      }

      statement.execute("INSERT INTO root.restart_delete.d1(time,s1,s2) VALUES(5,15,105)");
      statement.execute("FLUSH");
      try (ResultSet rows =
          statement.executeQuery("SELECT s1,s2 FROM root.restart_delete.d1 ORDER BY time")) {
        TestUtils.assertResultSetEqual(
            rows,
            "Time,root.restart_delete.d1.s1,root.restart_delete.d1.s2,",
            new String[] {"1,null,101,", "2,1202,null,", "4,14,104,", "5,15,105,"});
      }
    }
  }
}
