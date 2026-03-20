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

import org.apache.iotdb.it.env.cluster.env.SimpleEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.exception.InconsistentDataException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS;
import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;
import static org.junit.Assert.assertEquals;

/** Cluster IT: SHOW TIMESERIES after DELETE TIMESERIES while data nodes stop one by one. */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBShowTimeseriesAfterDeleteDataNodeDownIT {

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
    simpleEnv.initClusterEnvironment(1, 3);

    try (Connection connection = simpleEnv.getAvailableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO root.db1.d1 (time, s1, s2, s3) VALUES (0, 1, 2, 3)");
      statement.execute("FLUSH");

      statement.execute("INSERT INTO root.db1.d1 (time, s1, s2, s3) VALUES (10, 11, 12, 13)");

      statement.execute("DELETE TIMESERIES root.db1.d1.s1");

      statement.execute("FLUSH");
    }

    try {
      for (DataNodeWrapper dataNodeWrapper : simpleEnv.getDataNodeWrapperList()) {
        dataNodeWrapper.stop();

        try (Connection connectionAfterNodeDown = simpleEnv.getAvailableConnection();
            Statement statementAfterNodeDown = connectionAfterNodeDown.createStatement()) {
          int count = 0;
          try (ResultSet resultSet = statementAfterNodeDown.executeQuery("SHOW TIMESERIES")) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
              StringBuilder row = new StringBuilder();
              for (int i = 0; i < metaData.getColumnCount(); i++) {
                row.append(resultSet.getString(i + 1)).append(",");
              }
              System.out.println(row);
              count++;
            }
          }
          assertEquals(4, count);
        }
        dataNodeWrapper.start();
      }
    } catch (InconsistentDataException e) {
      // ignore
    } catch (SQLException e) {
      if (!e.getMessage().contains("Maybe server is down")) {
        throw e;
      }
    } finally {
      simpleEnv.cleanClusterEnvironment();
    }
  }
}
