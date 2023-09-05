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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBInsertWithoutTimeIT {

  private static final List<String> sqls =
      Arrays.asList(
          "CREATE DATABASE root.sg1",
          "CREATE TIMESERIES root.sg1.d1.s1 INT64",
          "CREATE TIMESERIES root.sg1.d1.s2 FLOAT",
          "CREATE TIMESERIES root.sg1.d1.s3 TEXT");

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeseries();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void createTimeseries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertWithoutTime() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg1.d1(s1, s2, s3) values (1, 1, '1')");
      Thread.sleep(1);
      statement.execute("insert into root.sg1.d1(s2, s1, s3) values (2, 2, '2')");
      Thread.sleep(1);
      statement.execute("insert into root.sg1.d1(s3, s2, s1) values ('3', 3, 3)");
      Thread.sleep(1);
      statement.execute("insert into root.sg1.d1(s1) values (1)");
      statement.execute("insert into root.sg1.d1(s2) values (2)");
      statement.execute("insert into root.sg1.d1(s3) values ('3')");
    } catch (SQLException | InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    String expectedHeader = "count(root.sg1.d1.s1),count(root.sg1.d1.s2),count(root.sg1.d1.s3),";
    String[] retArray = new String[] {"4,4,4,"};
    resultSetEqualTest(
        "select count(s1), count(s2), count(s3) from root.sg1.d1", expectedHeader, retArray);
  }

  @Test
  public void testInsertWithoutValueColumns() {
    assertNonQueryTestFail(
        "insert into root.sg1.d1(time) values (1)", "Error occurred while parsing SQL");
  }

  @Test
  public void testInsertMultiRow() {
    assertNonQueryTestFail(
        "insert into root.sg1.d1(s3) values ('1'), ('2')",
        "need timestamps when insert multi rows");
    assertNonQueryTestFail(
        "insert into root.sg1.d1(s1, s2) values (1, 1), (2, 2)",
        "need timestamps when insert multi rows");
  }

  @Test
  public void testInsertWithMultiTimesColumns() {
    assertNonQueryTestFail(
        "insert into root.sg1.d1(time, time) values (1, 1)", "Error occurred while parsing SQL");
    assertNonQueryTestFail(
        "insert into root.sg1.d1(time, s1, time) values (1, 1, 1)",
        "Error occurred while parsing SQL");
  }
}
