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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.cluster.env.SimpleEnv;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS;
import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/** Cluster IT: SHOW TIMESERIES after DELETE TIMESERIES while data nodes stop one by one. */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBShowTimeseriesAfterDeleteDataNodeDownIT {

  private static final String SHOW_TIMESERIES = "SHOW TIMESERIES root.db1.**";
  private static final Set<String> EXPECTED_TIMESERIES =
      new HashSet<>(Arrays.asList("root.db1.d1.s2", "root.db1.d1.s3"));

  @Test
  public void testShowTimeseriesAfterDeleteTimeseriesWhenDataNodesStopOneByOne()
      throws SQLException {
    SimpleEnv simpleEnv = new SimpleEnv();
    simpleEnv
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(IOT_CONSENSUS)
        .setDataReplicationFactor(3)
        .setSchemaRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setSchemaReplicationFactor(3);

    try {
      simpleEnv.initClusterEnvironment(1, 3);

      try (Connection connection = simpleEnv.getAvailableConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("INSERT INTO root.db1.d1 (time, s1, s2, s3) VALUES (0, 1, 2, 3)");
        statement.execute("FLUSH");

        statement.execute("INSERT INTO root.db1.d1 (time, s1, s2, s3) VALUES (10, 11, 12, 13)");

        statement.execute("DELETE TIMESERIES root.db1.d1.s1");

        statement.execute("FLUSH");
      }

      for (int i = 0; i < simpleEnv.getDataNodeWrapperList().size(); i++) {
        simpleEnv.shutdownDataNode(i);
        waitUntilOneNodeUnknown(simpleEnv);
        assertShowTimeseriesEventually(simpleEnv);

        simpleEnv.startDataNode(i);
        simpleEnv.checkClusterStatusWithoutUnknown();
        assertShowTimeseriesEventually(simpleEnv);
      }
    } finally {
      simpleEnv.cleanClusterEnvironment();
    }
  }

  private static void waitUntilOneNodeUnknown(final SimpleEnv simpleEnv) {
    simpleEnv.checkClusterStatus(
        nodeStatus ->
            nodeStatus.values().stream().filter(NodeStatus.Unknown.getStatus()::equals).count() == 1
                && nodeStatus.values().stream()
                        .filter(NodeStatus.Running.getStatus()::equals)
                        .count()
                    == nodeStatus.size() - 1,
        processStatus ->
            processStatus.values().stream().filter(status -> status == 0).count()
                == processStatus.size() - 1);
  }

  private static void assertShowTimeseriesEventually(final SimpleEnv simpleEnv) {
    await()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(2L, TimeUnit.MINUTES)
        .untilAsserted(() -> assertShowTimeseries(simpleEnv));
  }

  private static void assertShowTimeseries(final SimpleEnv simpleEnv) throws SQLException {
    final Set<String> actualTimeseries = new HashSet<>();
    try (Connection connection = simpleEnv.getAvailableConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(SHOW_TIMESERIES)) {
      while (resultSet.next()) {
        actualTimeseries.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
      }
    }
    assertEquals(EXPECTED_TIMESERIES, actualTimeseries);
  }
}
