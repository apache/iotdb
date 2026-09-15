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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.exception.InconsistentDataException;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSetSystemStatusTableIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void setSystemStatus() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("SET SYSTEM TO READONLY ON CLUSTER");
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollDelay(1, TimeUnit.SECONDS)
          .until(
              () -> {
                ResultSet resultSet = statement.executeQuery("SHOW DATANODES");
                int num = 0;
                int manualReasonNum = 0;
                try {
                  while (resultSet.next()) {
                    String status = resultSet.getString("Status");
                    if (status.equals("ReadOnly")) {
                      num++;
                    }
                    // The ReadOnly requested through SQL is reported with the Manual reason.
                    // The StatusReason column only appears once at least one node has reported
                    // its reason, so a missing column means the condition is not satisfied yet.
                    if ("Manual".equals(resultSet.getString("StatusReason"))) {
                      manualReasonNum++;
                    }
                  }
                } catch (Exception e) {
                  // The StatusReason column (and the Manual reason behind it) may not be
                  // propagated yet, or the cluster may be transiently inconsistent while the
                  // status is changing: treat both as "not satisfied yet".
                  return false;
                }
                return num == EnvFactory.getEnv().getDataNodeWrapperList().size()
                    && manualReasonNum == EnvFactory.getEnv().getDataNodeWrapperList().size();
              });

      // The table-model NODES view keeps the status and its reason in two separate columns
      // instead of merging them into "ReadOnly(Manual)".
      try (ResultSet nodesResultSet =
          statement.executeQuery("select * from information_schema.nodes")) {
        int dataNodeNum = 0;
        while (nodesResultSet.next()) {
          if ("DataNode".equals(nodesResultSet.getString("node_type"))) {
            dataNodeNum++;
            Assert.assertEquals("ReadOnly", nodesResultSet.getString("status"));
            Assert.assertEquals("Manual", nodesResultSet.getString("status_reason"));
          }
        }
        Assert.assertEquals(EnvFactory.getEnv().getDataNodeWrapperList().size(), dataNodeNum);
      }

      statement.execute("SET SYSTEM TO RUNNING ON CLUSTER");
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollDelay(1, TimeUnit.SECONDS)
          .until(
              () -> {
                ResultSet resultSet = statement.executeQuery("SHOW DATANODES");

                int num = 0;
                try {
                  while (resultSet.next()) {
                    String status = resultSet.getString("Status");
                    if (status.equals("Running")) {
                      num++;
                    }
                  }
                } catch (InconsistentDataException e) {
                  return false;
                }
                return num == EnvFactory.getEnv().getDataNodeWrapperList().size();
              });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
