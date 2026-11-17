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
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public abstract class IoTDBFuzzyQueryTableITBase extends IoTDBSelectCompareExpressionTableITBase {
  private static List<String> sqls = new ArrayList<>();

  protected static void initCreateSQLStatement() {
    sqls.add("CREATE DATABASE TestFuzzyQuery");
    sqls.add("USE TestFuzzyQuery");
    sqls.add(
        "CREATE TABLE likeTest(device_id STRING TAG, s1 TEXT FIELD, s2 STRING FIELD, s3 STRING FIELD)");
    sqls.add(
        "INSERT INTO likeTest(time, device_id, s1, s2, s3) VALUES(1,'d1', 'abcdef', 'a%', '\\')");
    sqls.add(
        "INSERT INTO likeTest(time, device_id, s1, s2, s3) VALUES(2,'d1', '_abcdef', '\\_a%','\\')");
    sqls.add(
        "INSERT INTO likeTest(time, device_id, s1, s2, s3) VALUES(3,'d1', 'abcdef%', '%_\\%','\\')");
  }

  protected static void insertData() throws SQLException {
    Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }
    statement.close();
  }

  // LIKE treats %, _, and an explicit escape character differently for the same source strings.
  // The plain pattern matches one value, while ESCAPE also matches the underscore and percent.
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
