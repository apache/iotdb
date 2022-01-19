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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

    prepareTemplate();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreateTemplateAndCreateTimeseries() throws SQLException {
    // test create schema template repeatedly
    try {
      statement.execute(
          "CREATE SCHEMA TEMPLATE temp1 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1 aligned (s1 FLOAT, s2 INT64))");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("303: Duplicated template name: temp1", e.getMessage());
    }

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE temp1 TO root.sg1.d1");

    // test drop template which has been set
    try {
      statement.execute("DROP SCHEMA TEMPLATE temp1");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals(
          "303: Template [temp1] has been set on MTree, cannot be dropped now.", e.getMessage());
    }

    statement.execute("SHOW TIMESERIES root.sg1.**");
    try (ResultSet resultSet = statement.getResultSet()) {
      Assert.assertFalse(resultSet.next());
    }

    // create timeseries of schema template
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d1");

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

    hasResult = statement.execute("SHOW DEVICES");
    Assert.assertTrue(hasResult);

    expectedResult = new String[] {"root.sg1.d1,false", "root.sg1.d1.vector1,true"};

    count = 0;
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String ActualResult =
            resultSet.getString("devices") + "," + resultSet.getString("isAligned");
        Assert.assertEquals(expectedResult[count], ActualResult);
        count++;
      }
    }
    Assert.assertEquals(2, count);

    try {
      statement.execute("UNSET SCHEMA TEMPLATE temp1 FROM root.sg1.d1");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("326: Template is in use on root.sg1.d1", e.getMessage());
    }
  }

  @Test
  public void testCreateAndSetSchemaTemplate() throws SQLException {
    // test create schema template repeatedly
    try {
      statement.execute(
          "CREATE SCHEMA TEMPLATE temp1 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1 aligned (s1 FLOAT, s2 INT64))");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("303: Duplicated template name: temp1", e.getMessage());
    }

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE temp1 TO root.sg1.d1");

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

    hasResult = statement.execute("SHOW DEVICES");
    Assert.assertTrue(hasResult);

    expectedResult = new String[] {"root.sg1.d1,false", "root.sg1.d1.vector1,true"};

    count = 0;
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String ActualResult =
            resultSet.getString("devices") + "," + resultSet.getString("isAligned");
        Assert.assertEquals(expectedResult[count], ActualResult);
        count++;
      }
    }
    Assert.assertEquals(2, count);

    try {
      statement.execute("UNSET SCHEMA TEMPLATE temp1 FROM root.sg1.d1");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("326: Template is in use on root.sg1.d1", e.getMessage());
    }
  }

  @Test
  public void testDropAndShowSchemaTemplates() throws SQLException {
    // show schema templates
    statement.execute("SHOW SCHEMA TEMPLATES");
    String[] expectedResult = new String[] {"temp1", "temp2", "temp3"};
    Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(expectedResultSet.contains(resultSet.getString("template name")));
        expectedResultSet.remove(resultSet.getString("template name"));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    // drop schema template
    statement.execute("DROP SCHEMA TEMPLATE temp2");
    statement.execute("SHOW SCHEMA TEMPLATES");
    expectedResult = new String[] {"temp1", "temp3"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(expectedResultSet.contains(resultSet.getString("template name")));
        expectedResultSet.remove(resultSet.getString("template name"));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());
  }

  @Test
  public void testShowNodesInSchemaTemplate() throws SQLException {
    // set schema template
    statement.execute("SHOW NODES IN SCHEMA TEMPLATE temp1");
    String[] expectedResult = new String[] {"s1", "vector1.s1", "vector1.s2"};
    Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(expectedResultSet.contains(resultSet.getString("child nodes")));
        expectedResultSet.remove(resultSet.getString("child nodes"));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());
  }

  @Test
  public void testShowPathsSetOrUsingSchemaTemplate() throws SQLException {
    // set schema template
    statement.execute("SET SCHEMA TEMPLATE temp1 TO root.sg1.d1");
    statement.execute("SET SCHEMA TEMPLATE temp1 TO root.sg1.d2");
    statement.execute("SET SCHEMA TEMPLATE temp1 TO root.sg2.d1");
    statement.execute("SET SCHEMA TEMPLATE temp1 TO root.sg2.d2");
    statement.execute("SET SCHEMA TEMPLATE temp2 TO root.sg3.d1");
    statement.execute("SET SCHEMA TEMPLATE temp2 TO root.sg3.d2");

    // activate schema template
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d2");
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg2.d1");

    // show paths set schema template
    statement.execute("SHOW PATHS SET SCHEMA TEMPLATE temp1");
    String[] expectedResult =
        new String[] {"root.sg1.d1", "root.sg2.d2", "root.sg1.d2", "root.sg2.d1"};
    Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(expectedResultSet.contains(resultSet.getString("child paths")));
        expectedResultSet.remove(resultSet.getString("child paths"));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    statement.execute("SHOW PATHS SET SCHEMA TEMPLATE temp2");
    expectedResult = new String[] {"root.sg3.d1", "root.sg3.d2"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(expectedResultSet.contains(resultSet.getString("child paths")));
        expectedResultSet.remove(resultSet.getString("child paths"));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    statement.execute("SHOW PATHS USING SCHEMA TEMPLATE temp1");
    expectedResult = new String[] {"root.sg1.d2", "root.sg2.d1"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(expectedResultSet.contains(resultSet.getString("child paths")));
        expectedResultSet.remove(resultSet.getString("child paths"));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    statement.execute("SHOW PATHS USING SCHEMA TEMPLATE temp2");
    ResultSet resultSet = statement.getResultSet();
    Assert.assertFalse(resultSet.next());
  }

  private void prepareTemplate() throws SQLException {
    // create storage group
    statement.execute("CREATE STORAGE GROUP root.sg1");
    statement.execute("CREATE STORAGE GROUP root.sg2");
    statement.execute("CREATE STORAGE GROUP root.sg3");

    // create schema template
    statement.execute(
        "CREATE SCHEMA TEMPLATE temp1 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1 aligned (s1 FLOAT, s2 INT64))");
    statement.execute(
        "CREATE SCHEMA TEMPLATE temp2 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1 aligned (s1 FLOAT, s2 INT64))");
    statement.execute(
        "CREATE SCHEMA TEMPLATE temp3 (s1 INT64 encoding=RLE compressor=SNAPPY, vector1 aligned (s1 FLOAT, s2 INT64))");
  }
}
