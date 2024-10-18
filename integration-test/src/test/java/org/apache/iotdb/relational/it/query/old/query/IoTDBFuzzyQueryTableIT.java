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

package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

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
import java.util.ArrayList;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFuzzyQueryTableIT {
  private static List<String> sqls = new ArrayList<>();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    initCreateSQLStatement();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void initCreateSQLStatement() {
    sqls.add("CREATE DATABASE TestFuzzyQuery");
    sqls.add("USE TestFuzzyQuery");
    sqls.add(
        "CREATE TABLE likeTest(device_id STRING ID, s1 TEXT MEASUREMENT, s2 STRING MEASUREMENT, s3 STRING MEASUREMENT)");
    sqls.add(
        "INSERT INTO likeTest(time, device_id, s1, s2, s3) VALUES(1,'d1', 'abcdef', 'a%', '\\')");
    sqls.add(
        "INSERT INTO likeTest(time, device_id, s1, s2, s3) VALUES(2,'d1', '_abcdef', '\\_a%','\\')");
    sqls.add(
        "INSERT INTO likeTest(time, device_id, s1, s2, s3) VALUES(3,'d1', 'abcdef%', '%_\\%','\\')");
  }

  private static void insertData() throws SQLException {
    Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }
    statement.close();
  }

  @Test
  public void testLike() throws SQLException {
    Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();
    statement.execute("USE TestFuzzyQuery");
    String[] ans = new String[] {"abcdef"};
    String query = "SELECT s1 FROM likeTest where s1 LIKE s2";
    try (ResultSet rs = statement.executeQuery(query)) {
      for (int i = 0; i < 1; i++) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(ans[i], rs.getString(1));
      }
      Assert.assertFalse(rs.next());
    }

    ans = new String[] {"abcdef", "_abcdef", "abcdef%"};
    query = "SELECT s1 FROM likeTest where s1 LIKE s2 ESCAPE s3";
    try (ResultSet rs = statement.executeQuery(query)) {
      for (int i = 0; i < 3; i++) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(ans[i], rs.getString(1));
      }
      Assert.assertFalse(rs.next());
    }
  }
}
