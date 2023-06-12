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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBViewQueryIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
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
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSelectIntoAliasSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.db");
      statement.execute("CREATE DATABASE root.view");

      statement.execute(
          "create aligned timeseries root.db.d1(s01 INT32 ENCODING=RLE COMPRESSOR=SNAPPY tags(city=beijing, description='this is a capital') attributes(speed=100, color=red),s02 INT64 ENCODING=RLE COMPRESSOR=SNAPPY tags(city=shanghai, description='this is a big city') attributes(speed=111, color=blue),s03 boolean,s04 float,s05 double,s06 text);");

      statement.execute("create view root.view.v1(c1,c2) as select s01,s02 from root.db.d1;");
      statement.execute(
          "alter view root.view.v1.c1 upsert tags(city=beijing, description='this is a capital') attributes(speed=100, color=red);");
      statement.execute(
          "alter view root.view.v1.c2 upsert tags(city=shanghai, description='this is a big city') attributes(speed=111, color=blue);");

      statement.execute(
          "insert into root.db.d1(time,s01,s02,s03,s04,s05,s06)aligned values(1000,123,456,true,1.2,1.3,\"one day\");");
      statement.execute(
          "insert into root.db.d1(time,s01,s02,s03,s04,s05,s06)aligned values(2000,222,444,false,2.3,4.5,\"two days\");");

      try (ResultSet resultSet =
          statement.executeQuery("select count(c1) from root.view.v1 group by tags(city);")) {
        StringBuilder stringBuilder = new StringBuilder();
        if (resultSet.next()) {
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals("beijing,2,", stringBuilder.toString());
        }

        Assert.assertFalse(resultSet.next());
      }

      statement.execute("create view root.view.v1.c3 as select s01+s02 from root.db.d1;");
      statement.execute(
          "alter view root.view.v1.c3 upsert tags(city=beijing, description='this is a capital') attributes(speed=100, color=red);");

      try {
        statement.executeQuery("select c3 from root.view.v1");
      } catch (SQLException e) {
        Assert.assertTrue(e.getMessage().contains("can't be used in group by tag."));
      }
    }
  }
}
