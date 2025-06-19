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

package org.apache.iotdb.db.it.schema.regionscan;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBActiveSchemaQueryIT extends AbstractSchemaIT {

  private static String[] sqls =
      new String[] {
        "create aligned timeseries root.sg.d1(s1(alias1) int32 tags('tag1'='v1', 'tag2'='v2'), s2 double attributes('attr3'='v3'))",
        "CREATE TIMESERIES root.sg.d0.s0 WITH DATATYPE=INT32",
        "CREATE TIMESERIES root.sg.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2",
        "CREATE TIMESERIES root.sg.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT, COMPDEV=0.01, COMPMINTIME=2, COMPMAXTIME=15",
        "CREATE DEVICE TEMPLATE t1 (s1 INT64 encoding=RLE compressor=SNAPPY, s2 INT32)",
        "CREATE DEVICE TEMPLATE t2 aligned(s1 INT64 encoding=RLE compressor=SNAPPY, s2 INT32)",
        "SET DEVICE TEMPLATE t1 to root.sg.d2",
        "SET DEVICE TEMPLATE t2 to root.sg.d3",
        "CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg.d2",
        "CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg.d3",
        "set ttl to root.sg.d3 60000",
      };

  public IoTDBActiveSchemaQueryIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    SchemaTestMode schemaTestMode = setUpEnvironment();
    if (schemaTestMode.equals(SchemaTestMode.PBTree)) {
      allocateMemoryForSchemaRegion(10000);
    }
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    createSchema();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  private void createSchema() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // 0. Check environment
      Set<String> expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d1.s1,alias1,root.sg,INT32,TS_2DIFF,LZ4,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,null,null,BASE,",
                  "root.sg.d1.s2,null,root.sg,DOUBLE,GORILLA,LZ4,null,{\"attr3\":\"v3\"},null,null,BASE,",
                  "root.sg.d0.s0,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.d0.s1,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=2},BASE,",
                  "root.sg.d0.s2,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=0.01, compmintime=2, compmaxtime=15},BASE,",
                  "root.sg.d2.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.sg.d2.s2,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.d3.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.sg.d3.s2,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,"));
      checkResultSet(statement, "show timeseries root.sg.**", expected);
      // 1. Without any data, result set should be empty
      try (ResultSet resultSet =
          statement.executeQuery("show timeseries root.sg.** where time>0")) {
        Assert.assertFalse(resultSet.next());
      }
      // 2. Insert data and check again.
      // - root.db.d0.* time=1
      // - root.db.d1.* time=2
      // - root.db.d2.* time=3
      // - root.db.d3.* time=now()
      statement.execute("insert into root.sg.d0(timestamp,s0, s1, s2) values(1,1,1,1)");
      statement.execute("insert into root.sg.d1(timestamp,s1,s2) values(2,2,2)");
      statement.execute("insert into root.sg.d2(timestamp,s1,s2) values(3,3,3)");
      statement.execute("insert into root.sg.d3(timestamp,s1,s2) values(now(),4,4)");
      expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d1.s1,alias1,root.sg,INT32,TS_2DIFF,LZ4,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,null,null,BASE,",
                  "root.sg.d1.s2,null,root.sg,DOUBLE,GORILLA,LZ4,null,{\"attr3\":\"v3\"},null,null,BASE,",
                  "root.sg.d0.s0,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.d0.s1,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=2},BASE,",
                  "root.sg.d0.s2,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=0.01, compmintime=2, compmaxtime=15},BASE,",
                  "root.sg.d2.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.sg.d2.s2,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.d3.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.sg.d3.s2,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,"));
      checkResultSet(statement, "show timeseries root.sg.** where time>0", expected);
      checkResultSet(
          statement,
          "count timeseries root.sg.** where time>0",
          new HashSet<>(Collections.singletonList("9,")));
      expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d1.s1,alias1,root.sg,INT32,TS_2DIFF,LZ4,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,null,null,BASE,",
                  "root.sg.d1.s2,null,root.sg,DOUBLE,GORILLA,LZ4,null,{\"attr3\":\"v3\"},null,null,BASE,",
                  "root.sg.d2.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.sg.d2.s2,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.d3.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.sg.d3.s2,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,"));
      checkResultSet(statement, "show timeseries root.sg.** where time>1", expected);
      checkResultSet(
          statement,
          "count timeseries root.sg.** where time>1",
          new HashSet<>(Collections.singletonList("6,")));
      expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d1.s1,alias1,root.sg,INT32,TS_2DIFF,LZ4,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,null,null,BASE,",
                  "root.sg.d1.s2,null,root.sg,DOUBLE,GORILLA,LZ4,null,{\"attr3\":\"v3\"},null,null,BASE,",
                  "root.sg.d2.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.sg.d2.s2,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.d3.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,",
                  "root.sg.d3.s2,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,"));
      checkResultSet(statement, "show timeseries root.sg.** where time!=1", expected);
      checkResultSet(
          statement,
          "count timeseries root.sg.** where time!=1",
          new HashSet<>(Collections.singletonList("6,")));
      expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d0.s0,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.d0.s1,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=2},BASE,",
                  "root.sg.d0.s2,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=0.01, compmintime=2, compmaxtime=15},BASE,"));
      checkResultSet(statement, "show timeseries root.sg.** where time<2", expected);
      checkResultSet(
          statement,
          "count timeseries root.sg.** where time<2",
          new HashSet<>(Collections.singletonList("3,")));
      expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d0.s0,null,root.sg,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,",
                  "root.sg.d0.s1,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=2},BASE,",
                  "root.sg.d0.s2,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=0.01, compmintime=2, compmaxtime=15},BASE,"));
      checkResultSet(statement, "show timeseries root.sg.** where time=1", expected);
      checkResultSet(
          statement,
          "count timeseries root.sg.** where time=1",
          new HashSet<>(Collections.singletonList("3,")));
      // 3. Check non-root user
      statement.execute("CREATE USER user1 'password123456'");
      statement.execute("GRANT READ_SCHEMA ON root.sg.d0.s1 TO USER user1");
      statement.execute("GRANT READ_SCHEMA ON root.sg.d3.s1 TO USER user1");
      try (Connection userCon = EnvFactory.getEnv().getConnection("user1", "password123456");
          Statement userStmt = userCon.createStatement()) {
        // 3.1 Without read data permission, result set should be empty
        try (ResultSet resultSet =
            userStmt.executeQuery("show timeseries root.sg.** where time>0")) {
          Assert.assertFalse(resultSet.next());
        }
        checkResultSet(
            userStmt,
            "count timeseries root.sg.** where time>0",
            new HashSet<>(Collections.singletonList("0,")));
      }
      statement.execute("GRANT READ_DATA ON root.sg.d0.s1 TO USER user1");
      statement.execute("GRANT READ_DATA ON root.sg.d3.s1 TO USER user1");
      try (Connection userCon = EnvFactory.getEnv().getConnection("user1", "password123456");
          Statement userStmt = userCon.createStatement()) {
        // 3.2 With read data permission, result set should not be empty
        expected =
            new HashSet<>(
                Arrays.asList(
                    "root.sg.d0.s1,null,root.sg,INT32,PLAIN,LZ4,null,null,SDT,{compdev=2},BASE,",
                    "root.sg.d3.s1,null,root.sg,INT64,RLE,SNAPPY,null,null,null,null,BASE,"));
        checkResultSet(userStmt, "show timeseries root.sg.** where time>0", expected);
        checkResultSet(
            userStmt,
            "count timeseries root.sg.** where time>0",
            new HashSet<>(Collections.singletonList("2,")));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testShowDevices() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // 0. Check environment
      Set<String> expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d0,false,null,INF,",
                  "root.sg.d1,true,null,INF,",
                  "root.sg.d2,false,t1,INF,",
                  "root.sg.d3,true,t2,60000,"));
      checkResultSet(statement, "show devices root.sg.*", expected);
      // 1. Without any data, result set should be empty
      try (ResultSet resultSet = statement.executeQuery("show devices root.sg.* where time>0")) {
        Assert.assertFalse(resultSet.next());
      }
      // 2. Insert data and check again.
      // - root.sg.d0.* time=1
      // - root.sg.d1.* time=2
      // - root.sg.d2.* time=3
      // - root.sg.d3.* time=now()
      statement.execute("insert into root.sg.d0(timestamp,s0, s1, s2) values(1,1,1,1)");
      statement.execute("insert into root.sg.d1(timestamp,s1,s2) values(2,2,2)");
      statement.execute("insert into root.sg.d2(timestamp,s1,s2) values(3,3,3)");
      statement.execute("insert into root.sg.d3(timestamp,s1,s2) values(now(),4,4)");
      expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d0,false,null,INF,",
                  "root.sg.d1,true,null,INF,",
                  "root.sg.d2,false,t1,INF,",
                  "root.sg.d3,true,t2,60000,"));
      checkResultSet(statement, "show devices root.sg.* where time>0", expected);
      checkResultSet(
          statement,
          "count devices root.sg.* where time>0",
          new HashSet<>(Collections.singletonList("4,")));
      expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d1,true,null,INF,",
                  "root.sg.d2,false,t1,INF,",
                  "root.sg.d3,true,t2,60000,"));
      checkResultSet(statement, "show devices root.sg.* where time>1", expected);
      checkResultSet(
          statement,
          "count devices root.sg.* where time>1",
          new HashSet<>(Collections.singletonList("3,")));
      expected =
          new HashSet<>(
              Arrays.asList(
                  "root.sg.d1,true,null,INF,",
                  "root.sg.d2,false,t1,INF,",
                  "root.sg.d3,true,t2,60000,"));
      checkResultSet(statement, "show devices root.sg.* where time!=1", expected);
      checkResultSet(
          statement,
          "count devices root.sg.* where time!=1",
          new HashSet<>(Collections.singletonList("3,")));
      expected = new HashSet<>(Collections.singletonList("root.sg.d0,false,null,INF,"));
      checkResultSet(statement, "show devices root.sg.* where time<2", expected);
      checkResultSet(
          statement,
          "count devices root.sg.* where time<2",
          new HashSet<>(Collections.singletonList("1,")));
      expected = new HashSet<>(Collections.singletonList("root.sg.d0,false,null,INF,"));
      checkResultSet(statement, "show devices root.sg.* where time=1", expected);
      checkResultSet(
          statement,
          "count devices root.sg.* where time=1",
          new HashSet<>(Collections.singletonList("1,")));
      // 3. Check non-root user
      statement.execute("CREATE USER user1 'password123456'");
      statement.execute("GRANT READ_SCHEMA ON root.sg.d0.s1 TO USER user1");
      statement.execute("GRANT READ_SCHEMA ON root.sg.d3.s1 TO USER user1");
      try (Connection userCon = EnvFactory.getEnv().getConnection("user1", "password123456");
          Statement userStmt = userCon.createStatement()) {
        // 3.1 Without read data permission, result set should be empty
        try (ResultSet resultSet = userStmt.executeQuery("show devices root.sg.* where time>0")) {
          Assert.assertFalse(resultSet.next());
        }
        checkResultSet(
            userStmt,
            "count devices root.sg.* where time>0",
            new HashSet<>(Collections.singletonList("0,")));
      }
      statement.execute("GRANT READ_DATA ON root.sg.d0.s1 TO USER user1");
      statement.execute("GRANT READ_DATA ON root.sg.d3.s1 TO USER user1");
      try (Connection userCon = EnvFactory.getEnv().getConnection("user1", "password123456");
          Statement userStmt = userCon.createStatement()) {
        // 3.2 With read data permission, result set should not be empty
        expected =
            new HashSet<>(Arrays.asList("root.sg.d0,false,null,INF,", "root.sg.d3,true,t2,60000,"));
        checkResultSet(userStmt, "show devices root.sg.* where time>0", expected);
        checkResultSet(
            userStmt,
            "count devices root.sg.* where time>0",
            new HashSet<>(Collections.singletonList("2,")));
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private void checkResultSet(Statement statement, String sql, Set<String> expected)
      throws Exception {
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      while (resultSet.next()) {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          builder.append(resultSet.getString(i)).append(",");
        }
        if (!expected.contains(builder.toString())) {
          System.out.println(builder.toString());
        }
        Assert.assertTrue(expected.contains(builder.toString()));
        expected.remove(builder.toString());
      }
    }
    Assert.assertTrue(expected.isEmpty());
  }
}
