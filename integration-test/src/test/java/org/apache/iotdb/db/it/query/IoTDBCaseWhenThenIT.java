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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCaseWhenThenIT {
  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "INSERT INTO root.sg.d1(timestamp,s1) values(0, 0)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(1, 11)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(2, 22)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(3, 33)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testKind1Easy() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String sql =
          "select case when s1=0 then 99 when s1>22 then 999 else 9999 end from root.sg.d1";
      ResultSet resultSet = statement.executeQuery(sql);
      resultSet.next();
      assertEquals("0", resultSet.getString(1));
      assertEquals("99.0", resultSet.getString(2));
      resultSet.next();
      assertEquals("1", resultSet.getString(1));
      assertEquals("9999.0", resultSet.getString(2));
      resultSet.next();
      assertEquals("2", resultSet.getString(1));
      assertEquals("9999.0", resultSet.getString(2));
      resultSet.next();
      assertEquals("3", resultSet.getString(1));
      assertEquals("999.0", resultSet.getString(2));
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    String[] expectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s1 AS INT32)"};
  }

  @Test
  public void testKind2Easy() {}

  @Test
  public void testWithoutElse() {}

  @Test
  public void testDifferentType() {}

  @Test
  public void testLargeNumberBranches() {}
}
