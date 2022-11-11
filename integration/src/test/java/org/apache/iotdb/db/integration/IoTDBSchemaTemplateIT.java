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

import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
          "CREATE SCHEMA TEMPLATE t1 (s1 INT64 encoding=RLE compressor=SNAPPY, s2 INT32)");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("303: Duplicated template name: t1", e.getMessage());
    }

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1.d1");
    statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg1.d2");

    // test drop template which has been set
    try {
      statement.execute("DROP SCHEMA TEMPLATE t1");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals(
          "303: Template [t1] has been set on MTree, cannot be dropped now.", e.getMessage());
    }

    statement.execute("SHOW TIMESERIES root.sg1.**");
    try (ResultSet resultSet = statement.getResultSet()) {
      Assert.assertFalse(resultSet.next());
    }

    // create timeseries of schema template
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d1");
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d2");

    boolean hasResult = statement.execute("SHOW TIMESERIES root.sg1.**");
    Assert.assertTrue(hasResult);

    Set<String> expectedResult =
        new HashSet<>(
            Arrays.asList(
                "root.sg1.d1.s1,INT64,RLE,SNAPPY",
                "root.sg1.d1.s2,DOUBLE,GORILLA,SNAPPY",
                "root.sg1.d2.s1,INT64,RLE,SNAPPY",
                "root.sg1.d2.s2,DOUBLE,GORILLA,SNAPPY"));

    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ENCODING)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ENCODING);
        Assert.assertTrue(expectedResult.contains(actualResult));
        expectedResult.remove(actualResult);
      }
    }
    Assert.assertTrue(expectedResult.isEmpty());

    hasResult = statement.execute("SHOW DEVICES");
    Assert.assertTrue(hasResult);

    expectedResult = new HashSet<>(Arrays.asList("root.sg1.d1,false", "root.sg1.d2,true"));

    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString(ColumnHeaderConstant.DEVICES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.IS_ALIGNED);
        Assert.assertTrue(expectedResult.contains(actualResult));
        expectedResult.remove(actualResult);
      }
    }
    Assert.assertTrue(expectedResult.isEmpty());

    try {
      statement.execute("UNSET SCHEMA TEMPLATE t1 FROM root.sg1.d1");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("326: Template is in use on root.sg1.d1", e.getMessage());
    }
  }

  @Test
  public void testCreateAndSetSchemaTemplate() throws SQLException {
    // test create schema template repeatedly
    try {
      statement.execute(
          "CREATE SCHEMA TEMPLATE t1 (s1 INT64 encoding=RLE compressor=SNAPPY, s2 INT32)");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("303: Duplicated template name: t1", e.getMessage());
    }

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1.d1");
    statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg1.d2");

    statement.execute("SHOW TIMESERIES root.sg1.**");
    try (ResultSet resultSet = statement.getResultSet()) {
      Assert.assertFalse(resultSet.next());
    }

    // set using schema template
    statement.execute("INSERT INTO root.sg1.d1(time,s1) VALUES (1,1)");
    statement.execute("INSERT INTO root.sg1.d2(time,s1) ALIGNED VALUES (1,1)");

    boolean hasResult = statement.execute("SHOW TIMESERIES root.sg1.**");
    Assert.assertTrue(hasResult);

    Set<String> expectedResult =
        new HashSet<>(
            Arrays.asList(
                "root.sg1.d1.s1,INT64,RLE,SNAPPY",
                "root.sg1.d1.s2,DOUBLE,GORILLA,SNAPPY",
                "root.sg1.d2.s1,INT64,RLE,SNAPPY",
                "root.sg1.d2.s2,DOUBLE,GORILLA,SNAPPY"));

    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ENCODING)
                + ","
                + resultSet.getString(ColumnHeaderConstant.COMPRESSION);
        Assert.assertTrue(expectedResult.contains(actualResult));
        expectedResult.remove(actualResult);
      }
    }
    Assert.assertTrue(expectedResult.isEmpty());

    hasResult = statement.execute("SHOW DEVICES");
    Assert.assertTrue(hasResult);

    expectedResult = new HashSet<>(Arrays.asList("root.sg1.d1,false", "root.sg1.d2,true"));

    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString(ColumnHeaderConstant.DEVICES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.IS_ALIGNED);
        Assert.assertTrue(expectedResult.contains(actualResult));
        expectedResult.remove(actualResult);
      }
    }
    Assert.assertTrue(expectedResult.isEmpty());

    try {
      statement.execute("UNSET SCHEMA TEMPLATE t1 FROM root.sg1.d1");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("326: Template is in use on root.sg1.d1", e.getMessage());
    }
  }

  @Test
  public void testDropAndShowSchemaTemplates() throws SQLException {
    // show schema templates
    statement.execute("SHOW SCHEMA TEMPLATES");
    String[] expectedResult = new String[] {"t1", "t2"};
    Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(
            expectedResultSet.contains(resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME)));
        expectedResultSet.remove(resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    // drop schema template
    statement.execute("DROP SCHEMA TEMPLATE t2");
    statement.execute("SHOW SCHEMA TEMPLATES");
    expectedResult = new String[] {"t1"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(
            expectedResultSet.contains(resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME)));
        expectedResultSet.remove(resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());
  }

  @Test
  public void testShowNodesInSchemaTemplate() throws SQLException {
    // set schema template
    statement.execute("SHOW NODES IN SCHEMA TEMPLATE t1");
    Set<String> expectedResultSet =
        new HashSet<>(Arrays.asList("s1,INT64,RLE,SNAPPY", "s2,DOUBLE,GORILLA,SNAPPY"));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString(ColumnHeaderConstant.CHILD_NODES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ENCODING)
                + ","
                + resultSet.getString(ColumnHeaderConstant.COMPRESSION);
        Assert.assertTrue(expectedResultSet.contains(actualResult));
        expectedResultSet.remove(actualResult);
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());
  }

  @Test
  public void testShowPathsSetOrUsingSchemaTemplate() throws SQLException {
    // set schema template
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1.d1");
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1.d2");
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg2.d1");
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg2.d2");
    statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg3.d1");
    statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg3.d2");

    // activate schema template
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d2");
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg2.d1");

    // show paths set schema template
    statement.execute("SHOW PATHS SET SCHEMA TEMPLATE t1");
    String[] expectedResult =
        new String[] {"root.sg1.d1", "root.sg2.d2", "root.sg1.d2", "root.sg2.d1"};
    Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(
            expectedResultSet.contains(resultSet.getString(ColumnHeaderConstant.CHILD_PATHS)));
        expectedResultSet.remove(resultSet.getString(ColumnHeaderConstant.CHILD_PATHS));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    statement.execute("SHOW PATHS SET SCHEMA TEMPLATE t2");
    expectedResult = new String[] {"root.sg3.d1", "root.sg3.d2"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(
            expectedResultSet.contains(resultSet.getString(ColumnHeaderConstant.CHILD_PATHS)));
        expectedResultSet.remove(resultSet.getString(ColumnHeaderConstant.CHILD_PATHS));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    statement.execute("SHOW PATHS USING SCHEMA TEMPLATE t1");
    expectedResult = new String[] {"root.sg1.d2", "root.sg2.d1"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        Assert.assertTrue(
            expectedResultSet.contains(resultSet.getString(ColumnHeaderConstant.CHILD_PATHS)));
        expectedResultSet.remove(resultSet.getString(ColumnHeaderConstant.CHILD_PATHS));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    statement.execute("SHOW PATHS USING SCHEMA TEMPLATE t2");
    ResultSet resultSet = statement.getResultSet();
    Assert.assertFalse(resultSet.next());
  }

  private void prepareTemplate() throws SQLException {
    // create database
    statement.execute("CREATE DATABASE root.sg1");
    statement.execute("CREATE DATABASE root.sg2");
    statement.execute("CREATE DATABASE root.sg3");

    // create schema template
    statement.execute("CREATE SCHEMA TEMPLATE t1 (s1 INT64, s2 DOUBLE)");
    statement.execute("CREATE SCHEMA TEMPLATE t2 aligned (s1 INT64, s2 DOUBLE)");
  }
}
