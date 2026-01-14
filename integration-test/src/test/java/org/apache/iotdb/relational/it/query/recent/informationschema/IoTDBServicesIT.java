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

package org.apache.iotdb.relational.it.query.recent.informationschema;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.commons.schema.table.InformationSchema.INFORMATION_DATABASE;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.itbase.env.BaseEnv.TABLE_SQL_DIALECT;
import static org.apache.iotdb.itbase.env.BaseEnv.TREE_SQL_DIALECT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBServicesIT {
  private static final String ADMIN_NAME =
      CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
  private static final String ADMIN_PWD =
      CommonDescriptor.getInstance().getConfig().getAdminPassword();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "TimechoDB@2021");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testQueryResult() {
    String[] retArray =
        new String[] {
          "MQTT,1,STOPPED,", "REST,1,STOPPED,",
        };

    // TableModel
    String[] header =
        new String[] {
          "service_name", "datanode_id", "state",
        };

    String sql = "SELECT * FROM services where datanode_id = 1";
    tableResultSetEqualTest(sql, header, retArray, INFORMATION_DATABASE);
    sql = "show services on 1";
    tableResultSetEqualTest(sql, header, retArray, INFORMATION_DATABASE);

    // TreeModel
    header =
        new String[] {
          "ServiceName", "DataNodeId", "State",
        };

    resultSetEqualTest(sql, header, retArray);
  }

  @Test
  public void testPrivilege() {
    testTargetModelPrivilege(TABLE_SQL_DIALECT);
    testTargetModelPrivilege(TREE_SQL_DIALECT);
  }

  private void testTargetModelPrivilege(String model) {
    String sql = "show services";
    try (Connection connection =
            EnvFactory.getEnv().getConnection("test", "TimechoDB@2021", model);
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
    } catch (SQLException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains("No permissions for this operation, please add privilege SYSTEM"));
    }

    try (Connection connection2 = EnvFactory.getEnv().getConnection(ADMIN_NAME, ADMIN_PWD, model);
        Statement statement2 = connection2.createStatement()) {
      statement2.executeQuery(sql);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
