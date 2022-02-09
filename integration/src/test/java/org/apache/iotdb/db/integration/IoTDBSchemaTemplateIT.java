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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.*;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneTest.class})
public class IoTDBSchemaTemplateIT {

  private Statement statement;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();

    Class.forName(Config.JDBC_DRIVER_NAME);
    connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  @Ignore
  public void testCreateTemplateAndCreateTimeseries() throws SQLException {
    statement.execute("CREATE STORAGE GROUP root.sg1");

    // create schema template
    statement.execute(
        "CREATE SCHEMA TEMPLATE temp1 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1(s1 FLOAT, s2 INT64))");

    // test create schema template repeatedly
    try {
      statement.execute(
          "CREATE SCHEMA TEMPLATE temp1 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1(s1 FLOAT, s2 INT64))");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("303: Duplicated template name: temp1", e.getMessage());
    }

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE temp1 TO root.sg1");

    statement.execute("SHOW TIMESERIES root.sg1.**");
    try (ResultSet resultSet = statement.getResultSet()) {
      Assert.assertFalse(resultSet.next());
    }

    // create timeseries of schema template
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1");

    boolean hasResult = statement.execute("SHOW TIMESERIES root.sg1.**");
    Assert.assertTrue(hasResult);

    String[] expectedResult =
        new String[] {
          "root.sg1.vector1.s1,FLOAT,GORILLA,SNAPPY",
          "root.sg1.vector1.s2,INT64,RLE,SNAPPY",
          "root.sg1.s1,INT64,RLE,SNAPPY"
        };

    int count = 0;
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String ActualResult =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression");
        Assert.assertEquals(expectedResult[count], ActualResult);
        count++;
      }
    }
    Assert.assertEquals(3, count);

    try {
      statement.execute("UNSET SCHEMA TEMPLATE temp1 FROM root.sg1");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("326: Template is in use on root.sg1", e.getMessage());
    }
  }

  @Test
  @Ignore
  public void testCreateAndSetSchemaTemplate() throws SQLException {
    statement.execute("CREATE STORAGE GROUP root.sg1");

    // create schema template
    statement.execute(
        "CREATE SCHEMA TEMPLATE temp1 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1(s1 FLOAT, s2 INT64))");

    // test create schema template repeatedly
    try {
      statement.execute(
          "CREATE SCHEMA TEMPLATE temp1 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1(s1 FLOAT, s2 INT64))");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("303: Duplicated template name: temp1", e.getMessage());
    }

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE temp1 TO root.sg1");

    statement.execute("SHOW TIMESERIES root.sg1.**");
    try (ResultSet resultSet = statement.getResultSet()) {
      Assert.assertFalse(resultSet.next());
    }

    // set using schema template
    statement.execute("INSERT INTO root.sg1.d1(time,s1) VALUES (1,1)");

    boolean hasResult = statement.execute("SHOW TIMESERIES root.sg1.**");
    Assert.assertTrue(hasResult);

    String[] expectedResult =
        new String[] {
          "root.sg1.d1.vector1.s1,FLOAT,GORILLA,SNAPPY",
          "root.sg1.d1.vector1.s2,INT64,RLE,SNAPPY",
          "root.sg1.d1.s1,INT64,RLE,SNAPPY"
        };

    int count = 0;
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String ActualResult =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression");
        Assert.assertEquals(expectedResult[count], ActualResult);
        count++;
      }
    }
    Assert.assertEquals(3, count);

    try {
      statement.execute("UNSET SCHEMA TEMPLATE temp1 FROM root.sg1");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("326: Template is in use on root.sg1.d1", e.getMessage());
    }
  }
}
