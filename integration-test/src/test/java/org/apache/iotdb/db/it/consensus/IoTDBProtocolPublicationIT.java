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

package org.apache.iotdb.db.it.consensus;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/** Single-point, three-node extraction acceptance without a performance workload. */
@Category(ClusterIT.class)
public class IoTDBProtocolPublicationIT {
  private boolean started;
  private final Map<Long, Long> expected = new TreeMap<>();

  @Before
  public void setUp() throws Exception {
    assumeTrue(Boolean.getBoolean("research.consensus.run"));
    String protocol =
        System.getProperty("research.protocol.class", ConsensusFactory.RAFT_CONSENSUS);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(protocol)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(3)
        .setEnableAuditLog(false)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setEnableAutoLeaderBalanceForRatisConsensus(false)
        .setEnableAutoLeaderBalanceForIoTConsensus(false);
    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeJVMConfig()
        .setInitHeapSize(256)
        .setMaxHeapSize(512);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeJVMConfig()
        .setInitHeapSize(512)
        .setMaxHeapSize(1024)
        .setMaxDirectMemorySize(512);
    Properties settings = new Properties();
    settings.setProperty("raft_apply_batch", "false");
    settings.setProperty("raft_pipeline_rows", "false");
    settings.setProperty("traft_apply_batch", "false");
    settings.setProperty("traft_pipeline_rows", "false");
    settings.setProperty("traft_apply_async", "false");
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("raft.")
          || key.startsWith("traft.")
          || key.startsWith("parallelraft.")
          || key.startsWith("lsmraft.")) {
        settings.setProperty(key.replace('.', '_'), System.getProperty(key));
      }
    }
    ((MppBaseConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(settings);
    started = true;
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() {
    if (started) {
      EnvFactory.getEnv().cleanClusterEnvironment();
    }
  }

  private void writeRange(long start, int count, boolean reverse) throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < count; i++) {
        long timestamp = start + (reverse ? count - 1 - i : i);
        long value = timestamp * 17 + 3;
        session.insertRecord(
            "root.publication.d",
            timestamp,
            Collections.singletonList("s"),
            Collections.singletonList(TSDataType.INT64),
            Collections.<Object>singletonList(value));
        expected.put(timestamp, value);
      }
    }
  }

  private void verifyEveryNode() throws Exception {
    for (DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
      long deadline = System.nanoTime() + 60_000_000_000L;
      for (; ; ) {
        Map<Long, Long> observed = new TreeMap<>();
        try (Connection connection =
                EnvFactory.getEnv().getConnection(node, "root", "root", "tree");
            Statement statement = connection.createStatement();
            ResultSet rows = statement.executeQuery("select s from root.publication.d")) {
          while (rows.next()) {
            observed.put(rows.getLong(1), rows.getLong(2));
          }
        }
        if (observed.equals(expected) || System.nanoTime() >= deadline) {
          assertEquals("Acknowledged values must survive on every endpoint", expected, observed);
          break;
        }
        Thread.sleep(100);
      }
    }
  }

  @Test
  public void acknowledgedSinglePointsSurviveRestart() throws Exception {
    try (Connection c = EnvFactory.getEnv().getConnection();
        Statement s = c.createStatement()) {
      s.execute("create database root.publication");
      s.execute("create timeseries root.publication.d.s with datatype=INT64, encoding=PLAIN");
    }
    writeRange(1000, 128, false);
    writeRange(1, 128, true);
    verifyEveryNode();
    TestUtils.stopForciblyAndRestartDataNodes();
    verifyEveryNode();
    writeRange(2000, 16, false);
    verifyEveryNode();
  }
}
