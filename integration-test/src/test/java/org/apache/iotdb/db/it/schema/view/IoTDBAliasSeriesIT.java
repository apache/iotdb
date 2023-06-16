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
package org.apache.iotdb.db.it.schema.view;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAliasSeriesIT {

  @BeforeClass
  public static void setUpCluster() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("DELETE DATABASE root.**");
      } catch (Exception e) {
        // If database is null, it will throw exception. Do nothing.
      }
    }
  }

  @Test
  public void testSelectIntoAliasSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.db");
      statement.execute("CREATE DATABASE root.view");

      statement.execute("create timeseries root.db.device.s01 with datatype=INT32");
      statement.execute("create timeseries root.db.device.s02 with datatype=INT32");
      statement.execute("CREATE VIEW root.view.device.status AS SELECT s01 FROM root.db.device");
      statement.execute("insert into root.db.device(time,s02) values(1,1)");

      try (ResultSet resultSet =
          statement.executeQuery("select s02 into root.view.device(status) from root.db.device")) {
        StringBuilder stringBuilder = new StringBuilder();
        if (resultSet.next()) {
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(
              "root.db.device.s02,root.view.device.status,1,", stringBuilder.toString());
        }
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("select status from root.view.device")) {
        StringBuilder stringBuilder = new StringBuilder();
        if (resultSet.next()) {
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals("1,1,", stringBuilder.toString());
        }
        Assert.assertFalse(resultSet.next());
      }
    }
  }
}
