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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// Todo: settle does not support mpp currently.
@Ignore
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSettleIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    initCreateSQLStatement();
    EnvFactory.getEnv().initClusterEnvironment();
    executeSql();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Test
  public void onlineSettleSGTest() {
    try (Statement statement = connection.createStatement()) {
      statement.execute("settle root.st1");
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  private static void close() throws SQLException {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void initCreateSQLStatement() {
    sqls.add("CREATE DATABASE root.st1");
    sqls.add("CREATE TIMESERIES root.st1.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    for (int i = 1; i <= 10; i++) {
      sqls.add("insert into root.st1.wf01.wt01(timestamp,status) values(" + 100 * i + ",false)");
    }
    sqls.add("flush");
    sqls.add("delete from root.st1.wf01.wt01.* where time<500");
  }

  private static void executeSql() throws ClassNotFoundException, SQLException {
    connection = EnvFactory.getEnv().getConnection();
    try (Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    }
  }
}
