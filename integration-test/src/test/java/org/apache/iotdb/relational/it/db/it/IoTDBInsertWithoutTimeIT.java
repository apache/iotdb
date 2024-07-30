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
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBInsertWithoutTimeIT {

  private static final List<String> sqls =
      Arrays.asList(
          "CREATE DATABASE test",
          "USE \"test\"",
          "CREATE TABLE sg1(id1 string id, s1 int64 measurement, s2 float measurement, s3 string measurement)");

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTable();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTable() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore // aggregation
  @Test
  public void testInsertWithoutTime() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("insert into sg1(id1, s1, s2, s3) values ('d1',1, 1, '1')");
      Thread.sleep(1);
      statement.execute("insert into sg1(id1, s2, s1, s3) values ('d1',2, 2, '2')");
      Thread.sleep(1);
      statement.execute("insert into sg1(id1, s3, s2, s1) values ('d1','3', 3, 3)");
      Thread.sleep(1);
      statement.execute("insert into sg1(id1, s1) values ('d1',1)");
      statement.execute("insert into sg1(id1, s2) values ('d1',2)");
      statement.execute("insert into sg1(id1, s3) values ('d1','3')");
    } catch (SQLException | InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    String expectedHeader = "count(s1),count(s2),count(s3),";
    String[] retArray = new String[] {"4,4,4,"};
    resultSetEqualTest("select count(s1), count(s2), count(s3) from sg1", expectedHeader, retArray);
  }

  @Test
  @Ignore // TODO: delete
  public void testInsertWithoutValueColumns() {
    assertTableNonQueryTestFail(
        "insert into sg1(id1, time) values ('d1', 1)",
        "InsertStatement should contain at least one measurement",
        "test");
  }

  @Test
  public void testInsertMultiRow() {
    assertTableNonQueryTestFail(
        "insert into sg1(s3) values ('d1', '1'), ('d1', '2')",
        "need timestamps when insert multi rows",
        "test");
    assertTableNonQueryTestFail(
        "insert into sg1(id1, s1, s2) values ('d1', 1, 1), ('d1', 2, 2)",
        "need timestamps when insert multi rows",
        "test");
  }

  @Test
  public void testInsertWithMultiTimesColumns() {
    assertTableNonQueryTestFail(
        "insert into sg1(id1, time, time) values ('d1', 1, 1)",
        "One row should only have one time value",
        "test");
    assertTableNonQueryTestFail(
        "insert into sg1(id1, time, s1, time) values ('d1', 1, 1, 1)",
        "One row should only have one time value",
        "test");
  }
}
