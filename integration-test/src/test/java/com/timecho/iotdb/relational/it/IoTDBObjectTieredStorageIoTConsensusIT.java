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

package com.timecho.iotdb.relational.it;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.env.CommonConfig;

import org.apache.tsfile.utils.Binary;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Cross-cutting coverage: IoTConsensus / PipeConsensus (IoTConsensusV2) replicas + OBJECT
 * multi-tier migration + DELETE / DROP TABLE physical cleanup.
 */
@RunWith(Parameterized.class)
@Category({TableClusterIT.class})
public class IoTDBObjectTieredStorageIoTConsensusIT {

  private static final String DATABASE = "tier_iot_obj";
  private static final byte[] OBJECT_V1 =
      new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe};
  private static final byte[] OBJECT_V2 =
      new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};

  private final String dataRegionConsensus;

  @Parameters(name = "dataRegionConsensus={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {ConsensusFactory.IOT_CONSENSUS}, {ConsensusFactory.IOT_CONSENSUS_V2},
        });
  }

  public IoTDBObjectTieredStorageIoTConsensusIT(String dataRegionConsensus) {
    this.dataRegionConsensus = dataRegionConsensus;
  }

  @Before
  public void setUp() throws Exception {
    CommonConfig commonConfig =
        EnvFactory.getEnv()
            .getConfig()
            .getCommonConfig()
            .setDataReplicationFactor(2)
            .setDataRegionConsensusProtocolClass(dataRegionConsensus);
    if (ConsensusFactory.IOT_CONSENSUS_V2.equals(dataRegionConsensus)) {
      commonConfig
          .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
          .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
          .setIoTConsensusV2Mode(ConsensusFactory.IOT_CONSENSUS_V2_STREAM_MODE);
    }
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setDnDataDirs("data/datanode/data/hot;data/datanode/data/cold");
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setTierTTLInMs("0;-1");
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Write OBJECT under the parameterized consensus protocol, wait TTL to migrate {@code .bin} off
   * hot onto cold, DELETE one sealed object, overwrite the surviving row, remigrate, then DROP
   * TABLE and assert no leftover bins.
   */
  @Test
  public void testDeleteAndDropAfterTierMigration() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + DATABASE);
      session.executeNonQueryStatement("USE \"" + DATABASE + "\"");
      session.executeNonQueryStatement(
          "CREATE TABLE camera(device_id STRING TAG, frame OBJECT FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO camera(time, device_id, frame) VALUES(1, 'd1', to_object(true, 0,"
              + " X'cafebabe'))");
      session.executeNonQueryStatement(
          "INSERT INTO camera(time, device_id, frame) VALUES(2, 'd1', to_object(true, 0,"
              + " X'deadbeef'))");
      session.executeNonQueryStatement("FLUSH");
    }

    awaitMigratedOffHot("both objects should leave hot after TTL migration");
    long coldBinsAfterMigration = countBinsOnTier("cold");
    Assert.assertTrue(
        "migrated objects should exist on cold across replicas (" + dataRegionConsensus + ")",
        coldBinsAfterMigration >= 2);
    assertReadObjectEverywhere(1L, OBJECT_V1);
    assertReadObjectEverywhere(2L, OBJECT_V2);

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"" + DATABASE + "\"");
      session.executeNonQueryStatement("DELETE FROM camera WHERE time = 1");
    }
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
                session.executeNonQueryStatement("USE \"" + DATABASE + "\"");
                try (SessionDataSet dataSet =
                    session.executeQueryStatement(
                        "SELECT time, READ_OBJECT(frame) AS payload FROM camera ORDER BY time")) {
                  SessionDataSet.DataIterator iterator = dataSet.iterator();
                  Assert.assertTrue(iterator.next());
                  Assert.assertEquals(2L, iterator.getLong("time"));
                  Assert.assertArrayEquals(OBJECT_V2, iterator.getBlob("payload").getValues());
                  Assert.assertFalse(iterator.next());
                }
              }
            });

    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              Assert.assertEquals(
                  "deleted object must leave no hot-tier bins", 0, countBinsOnTier("hot"));
              Assert.assertTrue(
                  "surviving object should still exist on cold after partial DELETE",
                  countBinsOnTier("cold") >= 1);
              assertReadObjectEverywhere(2L, OBJECT_V2);
            });

    // Overwrite surviving timestamp on hot, then remigrate; stale cold payload must not win reads.
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"" + DATABASE + "\"");
      session.executeNonQueryStatement(
          "INSERT INTO camera(time, device_id, frame) VALUES(2, 'd1', to_object(true, 0,"
              + " X'cafebabe'))");
      session.executeNonQueryStatement("FLUSH");
    }
    assertReadObjectEverywhere(2L, OBJECT_V1);
    awaitMigratedOffHot("overwritten object should remigrate off hot");
    assertReadObjectEverywhere(2L, OBJECT_V1);

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"" + DATABASE + "\"");
      session.executeNonQueryStatement("DROP TABLE camera");
    }
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              Assert.assertEquals(0, countBinsOnTier("hot"));
              Assert.assertEquals(0, countBinsOnTier("cold"));
            });
  }

  private static void awaitMigratedOffHot(String message) {
    Awaitility.await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertEquals(message + " (hot must be empty)", 0, countBinsOnTier("hot"));
              Assert.assertTrue(message + " (cold must have bins)", countBinsOnTier("cold") > 0);
            });
  }

  private static void assertReadObjectEverywhere(long time, byte[] expected) throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"" + DATABASE + "\"");
      assertReadObject(session, time, expected);
    }
    // Replicas that actually host object files must serve the same payload.
    for (DataNodeWrapper wrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      if (!wrapper.isAlive()) {
        continue;
      }
      if (countBinsOnNodeTier(wrapper, "cold") == 0 && countBinsOnNodeTier(wrapper, "hot") == 0) {
        continue;
      }
      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      wrapper,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE \"" + DATABASE + "\"");
        try (java.sql.ResultSet rs =
            statement.executeQuery("SELECT READ_OBJECT(frame) FROM camera WHERE time = " + time)) {
          Assert.assertTrue("replica " + wrapper.getId() + " missing row time=" + time, rs.next());
          Assert.assertArrayEquals(
              "replica " + wrapper.getId() + " payload mismatch for time=" + time,
              expected,
              rs.getBytes(1));
          Assert.assertFalse(rs.next());
        }
      }
    }
  }

  private static void assertReadObject(ITableSession session, long time, byte[] expected)
      throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT READ_OBJECT(frame) FROM camera WHERE time = " + time)) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      Assert.assertTrue(iterator.next());
      Binary binary = iterator.getBlob(1);
      Assert.assertArrayEquals(expected, binary.getValues());
      Assert.assertFalse(iterator.next());
    }
  }

  private static long countBinsOnTier(String tierName) throws Exception {
    long total = 0;
    List<DataNodeWrapper> wrappers = EnvFactory.getEnv().getDataNodeWrapperList();
    for (DataNodeWrapper wrapper : wrappers) {
      total += countBinsOnNodeTier(wrapper, tierName);
    }
    return total;
  }

  private static long countBinsOnNodeTier(DataNodeWrapper wrapper, String tierName)
      throws Exception {
    File root =
        new File(
            wrapper.getDataNodeDir()
                + File.separator
                + "data"
                + File.separator
                + tierName
                + File.separator
                + "object");
    if (!root.exists()) {
      return 0;
    }
    try (Stream<Path> stream = Files.walk(root.toPath())) {
      return stream
          .filter(Files::isRegularFile)
          .map(path -> path.getFileName().toString())
          .filter(name -> name.endsWith(".bin") && !name.endsWith(".bin.tmp"))
          .count();
    }
  }
}
