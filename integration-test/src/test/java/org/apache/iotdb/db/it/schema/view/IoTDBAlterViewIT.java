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
public class IoTDBAlterViewIT {

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
  public void testAlterView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.db1.d.s1 with datatype=INT32");
      statement.execute("create timeseries root.db1.d.s2 with datatype=INT32");
      statement.execute("create timeseries root.db1.d.s3 with datatype=INT32");
      statement.execute("create timeseries root.db2.d.s1 with datatype=INT32");
      statement.execute("create timeseries root.db2.d.s2 with datatype=INT32");
      statement.execute("create timeseries root.db2.d.s3 with datatype=INT32");

      statement.execute(
          "create view root(view1.d.s, view2.d.s, view3.d.s, view4.d.s, view5.d.s, view6.d.s) as root(db1.d.s1, db1.d.s2, db1.d.s3, db2.d.s1, db2.d.s2, db2.d.s3)");

      String[][] map =
          new String[][] {
            new String[] {"root.view1.d.s", "root.db1.d.s1"},
            new String[] {"root.view2.d.s", "root.db1.d.s2"},
            new String[] {"root.view3.d.s", "root.db1.d.s3"},
            new String[] {"root.view4.d.s", "root.db2.d.s1"},
            new String[] {"root.view5.d.s", "root.db2.d.s2"},
            new String[] {"root.view6.d.s", "root.db2.d.s3"},
          };
      for (String[] strings : map) {
        try (ResultSet resultSet =
            statement.executeQuery(String.format("show view %s", strings[0]))) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(strings[1], resultSet.getString("Source"));
        }
      }

      statement.execute(
          "alter view root(view1.d.s, view2.d.s, view3.d.s, view4.d.s, view5.d.s, view6.d.s) as root(db2.d.s2, db2.d.s3, db2.d.s1, db1.d.s2, db1.d.s3, db1.d.s1)");

      map =
          new String[][] {
            new String[] {"root.view1.d.s", "root.db2.d.s2"},
            new String[] {"root.view2.d.s", "root.db2.d.s3"},
            new String[] {"root.view3.d.s", "root.db2.d.s1"},
            new String[] {"root.view4.d.s", "root.db1.d.s2"},
            new String[] {"root.view5.d.s", "root.db1.d.s3"},
            new String[] {"root.view6.d.s", "root.db1.d.s1"},
          };
      for (String[] strings : map) {
        try (ResultSet resultSet =
            statement.executeQuery(String.format("show view %s", strings[0]))) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(strings[1], resultSet.getString("Source"));
        }
      }
    }
  }
}
