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
package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
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
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBSchemaTemplateIT {

  private Statement statement;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
    connection = EnvFactory.getEnv().getConnection();
    statement = connection.createStatement();

    prepareTemplate();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void testCreateTemplateAndCreateTimeseries() throws SQLException {
    // test create schema template repeatedly
    try {
      statement.execute(
          "CREATE SCHEMA TEMPLATE t1 (s1 INT64 encoding=RLE compressor=SNAPPY, s2 INT32)");
    } catch (SQLException e) {
      Assert.assertEquals("303: Duplicated template name: t1", e.getMessage());
    }

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1.d1");
    statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg1.d2");

    // test drop template which has been set
    //    try {
    //      statement.execute("DROP SCHEMA TEMPLATE t1");
    //    } catch (SQLException e) {
    //      Assert.assertEquals(
    //          "303: Template [t1] has been set on MTree, cannot be dropped now.", e.getMessage());
    //    }

    try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.**")) {
      Assert.assertFalse(resultSet.next());
    }

    // create timeseries of schema template
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d1");
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d2");

    Set<String> expectedResult =
        new HashSet<>(
            Arrays.asList(
                "root.sg1.d1.s1,INT64,RLE,SNAPPY",
                "root.sg1.d1.s2,DOUBLE,GORILLA,SNAPPY",
                "root.sg1.d2.s1,INT64,RLE,SNAPPY",
                "root.sg1.d2.s2,DOUBLE,GORILLA,SNAPPY"));

    try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.**"); ) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression");
        Assert.assertTrue(expectedResult.contains(actualResult));
        expectedResult.remove(actualResult);
      }
    }
    Assert.assertTrue(expectedResult.isEmpty());

    try (ResultSet resultSet = statement.executeQuery("COUNT TIMESERIES root.sg1.**")) {
      resultSet.next();
      Assert.assertEquals(4, resultSet.getInt(1));
    }

    expectedResult = new HashSet<>(Arrays.asList("root.sg1.d1,false", "root.sg1.d2,true"));

    try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES")) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString("devices") + "," + resultSet.getString("isAligned");
        Assert.assertTrue(expectedResult.contains(actualResult));
        expectedResult.remove(actualResult);
      }
    }
    Assert.assertTrue(expectedResult.isEmpty());

    //    try {
    //      statement.executeQuery("UNSET SCHEMA TEMPLATE t1 FROM root.sg1.d1");
    //    } catch (SQLException e) {
    //      Assert.assertEquals("326: Template is in use on root.sg1.d1", e.getMessage());
    //    }
  }

  @Test
  public void testCreateAndSetSchemaTemplate() throws SQLException {
    // test create schema template repeatedly
    try {
      statement.execute(
          "CREATE SCHEMA TEMPLATE t1 (s1 INT64 encoding=RLE compressor=SNAPPY, s2 INT32)");
    } catch (SQLException e) {
      Assert.assertEquals("303: Duplicated template name: t1", e.getMessage());
    }

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1.d1");
    statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg1.d2");

    try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.**")) {
      Assert.assertFalse(resultSet.next());
    }

    // set using schema template
    statement.execute("INSERT INTO root.sg1.d1(time,s1) VALUES (1,1)");
    statement.execute("INSERT INTO root.sg1.d2(time,s1) ALIGNED VALUES (1,1)");

    Set<String> expectedResult =
        new HashSet<>(
            Arrays.asList(
                "root.sg1.d1.s1,INT64,RLE,SNAPPY",
                "root.sg1.d1.s2,DOUBLE,GORILLA,SNAPPY",
                "root.sg1.d2.s1,INT64,RLE,SNAPPY",
                "root.sg1.d2.s2,DOUBLE,GORILLA,SNAPPY"));

    try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.**")) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression");
        Assert.assertTrue(expectedResult.contains(actualResult));
        expectedResult.remove(actualResult);
      }
    }
    Assert.assertTrue(expectedResult.isEmpty());

    try (ResultSet resultSet = statement.executeQuery("COUNT TIMESERIES root.sg1.**")) {
      resultSet.next();
      Assert.assertEquals(4, resultSet.getInt(1));
    }

    expectedResult = new HashSet<>(Arrays.asList("root.sg1.d1,false", "root.sg1.d2,true"));

    try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES")) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString("devices") + "," + resultSet.getString("isAligned");
        Assert.assertTrue(expectedResult.contains(actualResult));
        expectedResult.remove(actualResult);
      }
    }
    Assert.assertTrue(expectedResult.isEmpty());

    //    try {
    //      statement.executeQuery("UNSET SCHEMA TEMPLATE t1 FROM root.sg1.d1");
    //    } catch (SQLException e) {
    //      Assert.assertEquals("326: Template is in use on root.sg1.d1", e.getMessage());
    //    }
  }

  @Ignore
  @Test
  public void testDropAndShowSchemaTemplates() throws SQLException {
    // show schema templates
    String[] expectedResult = new String[] {"t1", "t2"};
    Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.executeQuery("SHOW SCHEMA TEMPLATES")) {
      while (resultSet.next()) {
        Assert.assertTrue(expectedResultSet.contains(resultSet.getString("template name")));
        expectedResultSet.remove(resultSet.getString("template name"));
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    // drop schema template
    statement.execute("DROP SCHEMA TEMPLATE t2");
    expectedResult = new String[] {"t1"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.executeQuery("SHOW SCHEMA TEMPLATES")) {
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
    Set<String> expectedResultSet =
        new HashSet<>(Arrays.asList("s1,INT64,RLE,SNAPPY", "s2,DOUBLE,GORILLA,SNAPPY"));
    try (ResultSet resultSet = statement.executeQuery("SHOW NODES IN SCHEMA TEMPLATE t1")) {
      while (resultSet.next()) {
        String actualResult =
            resultSet.getString("child nodes")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression");
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
    statement.execute("INSERT INTO root.sg3.d2.verify(time, show) VALUES (1, 1)");

    try (ResultSet resultSet = statement.executeQuery("SHOW PATHS USING SCHEMA TEMPLATE t1")) {
      String resultRecord;
      while (resultSet.next()) {
        resultRecord = resultSet.getString(1);
        Assert.assertEquals("", resultRecord);
      }
    }

    // activate schema template
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d2");
    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg2.d1");

    // show paths set schema template
    String[] expectedResult =
        new String[] {"root.sg1.d1", "root.sg2.d2", "root.sg1.d2", "root.sg2.d1"};
    Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET SCHEMA TEMPLATE t1")) {
      String resultRecord;
      while (resultSet.next()) {
        resultRecord = resultSet.getString(1);
        Assert.assertTrue(expectedResultSet.contains(resultRecord));
        expectedResultSet.remove(resultRecord);
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    expectedResult = new String[] {"root.sg3.d1", "root.sg3.d2"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET SCHEMA TEMPLATE t2")) {
      String resultRecord;
      while (resultSet.next()) {
        resultRecord = resultSet.getString(1);
        Assert.assertTrue(expectedResultSet.contains(resultRecord));
        expectedResultSet.remove(resultRecord);
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    expectedResult = new String[] {"root.sg1.d2", "root.sg2.d1"};
    expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (ResultSet resultSet = statement.executeQuery("SHOW PATHS USING SCHEMA TEMPLATE t1")) {
      String resultRecord;
      while (resultSet.next()) {
        resultRecord = resultSet.getString(1);
        Assert.assertTrue(expectedResultSet.contains(resultRecord));
        expectedResultSet.remove(resultRecord);
      }
    }
    Assert.assertEquals(0, expectedResultSet.size());

    ResultSet resultSet = statement.executeQuery("SHOW PATHS USING SCHEMA TEMPLATE t2");
    Assert.assertFalse(resultSet.next());
  }

  @Test
  public void testSetAndActivateTemplateOnSGNode() throws SQLException {
    statement.execute("CREATE STORAGE GROUP root.test.sg_satosg");
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.test.sg_satosg");
    statement.execute("INSERT INTO root.test.sg_satosg(time, s1) VALUES (1, 1)");
    statement.execute("INSERT INTO root.test.sg_satosg(time, s1) VALUES (2, 2)");
    ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.test.sg_satosg.**");

    Set<String> expRes =
        new HashSet<>(
            Arrays.asList(new String[] {"root.test.sg_satosg.s1", "root.test.sg_satosg.s2"}));
    int resCnt = 0;
    while (resultSet.next()) {
      resCnt++;
      expRes.remove(resultSet.getString("timeseries"));
    }
    Assert.assertEquals(2, resCnt);
    Assert.assertTrue(expRes.isEmpty());

    resultSet = statement.executeQuery("SELECT COUNT(s1) from root.test.sg_satosg");
    while (resultSet.next()) {
      Assert.assertEquals(2L, resultSet.getLong("COUNT(root.test.sg_satosg.s1)"));
    }
  }

  private void prepareTemplate() throws SQLException {
    // create storage group
    statement.execute("CREATE STORAGE GROUP root.sg1");
    statement.execute("CREATE STORAGE GROUP root.sg2");
    statement.execute("CREATE STORAGE GROUP root.sg3");

    // create schema template
    statement.execute("CREATE SCHEMA TEMPLATE t1 (s1 INT64, s2 DOUBLE)");
    statement.execute("CREATE SCHEMA TEMPLATE t2 aligned (s1 INT64, s2 DOUBLE)");
  }
}
