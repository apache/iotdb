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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.enhanced;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeAlterTableColumnNameIT {

  protected static BaseEnv senderEnv;
  protected static BaseEnv receiverEnv;

  @BeforeClass
  public static void setUpClass() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    ;

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(3)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    ;

    senderEnv.initClusterEnvironment(1, 3, 180);
    receiverEnv.initClusterEnvironment(1, 3, 180);
  }

  @AfterClass
  public static void tearDownClass() {
    if (senderEnv != null) {
      senderEnv.cleanClusterEnvironment();
    }
    if (receiverEnv != null) {
      receiverEnv.cleanClusterEnvironment();
    }
  }

  @Test
  public void testAlterTableName() throws IoTDBConnectionException, StatementExecutionException {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (ITableSession senderSession = senderEnv.getTableSessionConnection()) {
      try {
        // insert some data to t1 and flush
        senderSession.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS test_alter_table");
        senderSession.executeNonQueryStatement("USE test_alter_table");
        senderSession.executeNonQueryStatement("CREATE TABLE t1 (s1 INT32, s2 FLOAT, s3 BOOLEAN)");
        senderSession.executeNonQueryStatement(
            "INSERT INTO t1 (time, s1, s2, s3) VALUES (1, 1, 1.0, true)");
        senderSession.executeNonQueryStatement("FLUSH");
        // rename table t1 to t2
        senderSession.executeNonQueryStatement("ALTER TABLE t1 RENAME TO t2");

        // create a pipe
        senderSession.executeNonQueryStatement(
            String.format(
                "CREATE PIPE p1 WITH SINK ('sink'='iotdb-thrift-sink', 'node-urls' = '%s:%s')",
                receiverIp, receiverPort));

        // check that receiver has received data in t2

        TestUtils.assertDataEventuallyOnEnv(
            receiverEnv,
            "SELECT * FROM test_alter_table.t2",
            "time,s1,s2,s3,",
            Collections.singleton("1970-01-01T00:00:00.001Z,1,1.0,true,"),
            20,
            true);
      } finally {
        senderSession.executeNonQueryStatement("DROP PIPE p1");
        senderSession.executeNonQueryStatement("DROP DATABASE test_alter_table");
      }
    }
  }

  @Test
  public void testAlterColumnName() throws IoTDBConnectionException, StatementExecutionException {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (ITableSession senderSession = senderEnv.getTableSessionConnection()) {
      try {
        // insert some data to t1 and flush
        senderSession.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS test_alter_column");
        senderSession.executeNonQueryStatement("USE test_alter_column");
        senderSession.executeNonQueryStatement("CREATE TABLE t1 (s1 INT32, s2 FLOAT, s3 BOOLEAN)");
        senderSession.executeNonQueryStatement(
            "INSERT INTO t1 (time, s1, s2, s3) VALUES (1, 1, 1.0, true)");
        senderSession.executeNonQueryStatement("FLUSH");
        // rename column s1 to s4
        senderSession.executeNonQueryStatement("ALTER TABLE t1 RENAME COLUMN s1 TO s4");

        // create a pipe
        senderSession.executeNonQueryStatement(
            String.format(
                "CREATE PIPE p2 WITH SINK ('sink'='iotdb-thrift-sink', 'node-urls' = '%s:%s')",
                receiverIp, receiverPort));

        // check that receiver has received data in t2

        TestUtils.assertDataEventuallyOnEnv(
            receiverEnv,
            "SELECT * FROM test_alter_column.t1",
            "time,s4,s2,s3,",
            Collections.singleton("1970-01-01T00:00:00.001Z,1,1.0,true,"),
            20,
            true);
      } finally {
        senderSession.executeNonQueryStatement("DROP PIPE p2");
        senderSession.executeNonQueryStatement("DROP DATABASE test_alter_column");
      }
    }
  }
}
