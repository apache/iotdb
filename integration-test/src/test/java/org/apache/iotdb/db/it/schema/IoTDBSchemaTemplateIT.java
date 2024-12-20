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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSchemaTemplateIT extends AbstractSchemaIT {

  public IoTDBSchemaTemplateIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    prepareTemplate();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  private void prepareTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create database
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE DATABASE root.sg2");
      statement.execute("CREATE DATABASE root.sg3");

      // create device template
      statement.execute("CREATE DEVICE TEMPLATE t1 (s1 INT64, s2 DOUBLE)");
      statement.execute("CREATE DEVICE TEMPLATE t2 aligned (s1 INT64, s2 DOUBLE)");
      statement.execute("CREATE DEVICE TEMPLATE t3 aligned (s1 INT64)");
    }
  }

  @Test
  public void testCreateTemplateAndCreateTimeseries() throws SQLException {
    // test create device template repeatedly
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // test datatype and encoding check
      try {
        statement.execute(
            "CREATE DEVICE TEMPLATE str1 (s1 TEXT encoding=GORILLA compressor=SNAPPY, s2 INT32)");
        fail();
      } catch (SQLException e) {
        System.out.println(e.getMessage());
        Assert.assertEquals(
            TSStatusCode.CREATE_TEMPLATE_ERROR.getStatusCode()
                + ": create template error -encoding GORILLA does not support TEXT",
            e.getMessage());
      }

      try {
        statement.execute(
            "CREATE DEVICE TEMPLATE t1 (s1 INT64 encoding=RLE compressor=SNAPPY, s2 INT32)");
        fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.METADATA_ERROR.getStatusCode() + ": Duplicated template name: t1",
            e.getMessage());
      }

      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg1.d2");
      statement.execute("SET DEVICE TEMPLATE t3 TO root.sg1.d3");

      // test drop template which has been set
      try {
        statement.execute("DROP DEVICE TEMPLATE t1");
        fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.METADATA_ERROR.getStatusCode()
                + ": Template [t1] has been set on MTree, cannot be dropped now.",
            e.getMessage());
      }

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.**")) {
        Assert.assertFalse(resultSet.next());
      }

      // create timeseries of device template
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d1");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d2");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d3");

      Set<String> expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.sg1.d1.s1,INT64,TS_2DIFF,LZ4",
                  "root.sg1.d1.s2,DOUBLE,GORILLA,LZ4",
                  "root.sg1.d2.s1,INT64,TS_2DIFF,LZ4",
                  "root.sg1.d2.s2,DOUBLE,GORILLA,LZ4",
                  "root.sg1.d3.s1,INT64,TS_2DIFF,LZ4"));

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.**"); ) {
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

      try (ResultSet resultSet = statement.executeQuery("COUNT TIMESERIES root.sg1.**")) {
        resultSet.next();
        Assert.assertEquals(5, resultSet.getLong(1));
      }

      expectedResult =
          new HashSet<>(Arrays.asList("root.sg1.d1,false", "root.sg1.d2,true", "root.sg1.d3,true"));

      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES")) {
        while (resultSet.next()) {
          String actualResult =
              resultSet.getString(ColumnHeaderConstant.DEVICE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.IS_ALIGNED);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      try {
        statement.execute("UNSET DEVICE TEMPLATE t1 FROM root.sg1.d1");
        fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode() + ": Template is in use on root.sg1.d1",
            e.getMessage());
      }
    }
  }

  @Test
  public void testCreateAndSetSchemaTemplate() throws SQLException {
    // test create device template repeatedly
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "CREATE DEVICE TEMPLATE t1 (s1 INT64 encoding=RLE compressor=SNAPPY, s2 INT32)");
        fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.METADATA_ERROR.getStatusCode() + ": Duplicated template name: t1",
            e.getMessage());
      }

      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg1.d2");

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.**")) {
        Assert.assertFalse(resultSet.next());
      }

      // set using device template
      statement.execute("INSERT INTO root.sg1.d1(time,s1) VALUES (1,1)");
      statement.execute("INSERT INTO root.sg1.d2(time,s1) ALIGNED VALUES (1,1)");

      Set<String> expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.sg1.d1.s1,INT64,TS_2DIFF,LZ4",
                  "root.sg1.d1.s2,DOUBLE,GORILLA,LZ4",
                  "root.sg1.d2.s1,INT64,TS_2DIFF,LZ4",
                  "root.sg1.d2.s2,DOUBLE,GORILLA,LZ4"));

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.**")) {
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

      try (ResultSet resultSet = statement.executeQuery("COUNT TIMESERIES root.sg1.**")) {
        resultSet.next();
        Assert.assertEquals(4, resultSet.getLong(1));
      }

      expectedResult = new HashSet<>(Arrays.asList("root.sg1.d1,false", "root.sg1.d2,true"));

      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES")) {
        while (resultSet.next()) {
          String actualResult =
              resultSet.getString(ColumnHeaderConstant.DEVICE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.IS_ALIGNED);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      try {
        statement.execute("UNSET DEVICE TEMPLATE t1 FROM root.sg1.d1");
        fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode() + ": Template is in use on root.sg1.d1",
            e.getMessage());
      }
    }
  }

  @Test
  public void testDropAndShowSchemaTemplates() throws SQLException {
    // show device templates
    String[] expectedResult = new String[] {"t1", "t2", "t3"};
    Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICE TEMPLATES")) {
        while (resultSet.next()) {
          Assert.assertTrue(
              expectedResultSet.contains(resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME)));
          expectedResultSet.remove(resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME));
        }
      }
      Assert.assertEquals(0, expectedResultSet.size());

      // drop device template
      statement.execute("DROP DEVICE TEMPLATE t2");
      expectedResult = new String[] {"t1", "t3"};
      expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICE TEMPLATES")) {
        while (resultSet.next()) {
          Assert.assertTrue(
              expectedResultSet.contains(resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME)));
          expectedResultSet.remove(resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME));
        }
      }
      Assert.assertEquals(0, expectedResultSet.size());
    }
  }

  @Test
  public void testShowNodesInSchemaTemplate() throws SQLException {
    // set device template
    Set<String> expectedResultSet =
        new HashSet<>(Arrays.asList("s1,INT64,TS_2DIFF,LZ4", "s2,DOUBLE,GORILLA,LZ4"));
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW NODES IN DEVICE TEMPLATE t1")) {
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d2");
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg2.d1");
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg2.d2");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg3.d1");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg3.d2");
      statement.execute("INSERT INTO root.sg3.d2.verify(time, show) ALIGNED VALUES (1, 1)");

      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS USING DEVICE TEMPLATE t1")) {
        String resultRecord;
        while (resultSet.next()) {
          resultRecord = resultSet.getString(1);
          Assert.assertEquals("", resultRecord);
        }
      }

      // activate device template
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d2");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg2.d1");

      // show paths set device template
      String[] expectedResult =
          new String[] {"root.sg1.d1", "root.sg2.d2", "root.sg1.d2", "root.sg2.d1"};
      Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE t1")) {
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
      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE t2")) {
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
      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS USING DEVICE TEMPLATE t1")) {
        String resultRecord;
        while (resultSet.next()) {
          resultRecord = resultSet.getString(1);
          Assert.assertTrue(expectedResultSet.contains(resultRecord));
          expectedResultSet.remove(resultRecord);
        }
      }
      Assert.assertEquals(0, expectedResultSet.size());

      ResultSet resultSet = statement.executeQuery("SHOW PATHS USING DEVICE TEMPLATE t2");
      Assert.assertTrue(resultSet.next());
    }
  }

  @Test
  public void testSetAndActivateTemplateOnSGNode() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.test.sg_satosg");
      statement.execute("SET DEVICE TEMPLATE t1 TO root.test.sg_satosg");
      statement.execute("INSERT INTO root.test.sg_satosg(time, s1) VALUES (1, 1)");
      statement.execute("INSERT INTO root.test.sg_satosg(time, s1) VALUES (2, 2)");
      ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.test.sg_satosg.**");

      Set<String> expRes =
          new HashSet<>(
              Arrays.asList(new String[] {"root.test.sg_satosg.s1", "root.test.sg_satosg.s2"}));
      int resCnt = 0;
      while (resultSet.next()) {
        resCnt++;
        expRes.remove(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
      }
      Assert.assertEquals(2, resCnt);
      Assert.assertTrue(expRes.isEmpty());

      resultSet = statement.executeQuery("SELECT COUNT(s1) from root.test.sg_satosg");
      while (resultSet.next()) {
        Assert.assertEquals(2L, resultSet.getLong("COUNT(root.test.sg_satosg.s1)"));
      }
    }
  }

  @Test
  public void testDeleteTimeSeriesWhenUsingTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg1.d2");

      statement.execute("CREATE TIMESERIES root.sg3.d1.s1 INT64");

      // set using device template
      statement.execute("INSERT INTO root.sg1.d1(time,s1) VALUES (1,1)");
      statement.execute("INSERT INTO root.sg1.d2(time,s1) ALIGNED VALUES (1,1)");
      statement.execute("INSERT INTO root.sg3.d1(time,s1) VALUES (1,1)");

      Set<String> expectedResult = new HashSet<>(Collections.singletonList("1,1,1,1,"));

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.**")) {
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= 4; i++) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String actualResult = stringBuilder.toString();
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      statement.execute("DELETE TIMESERIES root.**.s1");

      expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.sg1.d1.s1,INT64,TS_2DIFF,LZ4", "root.sg1.d2.s1,INT64,TS_2DIFF,LZ4"));

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.**.s1")) {
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

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.**")) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSchemaQueryAndFetchWithUnrelatedTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DEVICE TEMPLATE t4 (s3 INT64, s4 DOUBLE)");

      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      statement.execute("SET DEVICE TEMPLATE t4 TO root.sg1.d2");

      // set using device template
      statement.execute("INSERT INTO root.sg1.d1(time,s1) VALUES (1,1)");
      statement.execute("INSERT INTO root.sg1.d2(time,s3) VALUES (1,1)");

      Set<String> expectedResult = new HashSet<>(Collections.singletonList("1,1,"));

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.**")) {
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= 2; i++) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String actualResult = stringBuilder.toString();
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      expectedResult =
          new HashSet<>(Collections.singletonList("root.sg1.d1.s1,INT64,TS_2DIFF,LZ4"));

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.**.s1")) {
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
    }
  }

  @Test
  public void testInsertDataWithMeasurementsBeyondTemplate() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      // insert data and auto activate device template
      statement.execute("INSERT INTO root.sg1.d1(time,s1,s2) VALUES (1,1,1)");
      // insert twice to make sure the timeseries in template has been cached
      statement.execute("INSERT INTO root.sg1.d1(time,s1,s2) VALUES (2,1,1)");

      // insert data with extra measurement s3 which should be checked by schema fetch and auto
      // created
      statement.execute("INSERT INTO root.sg1.d1(time,s1,s2,s3) VALUES (2,1,1,1)");

      try (ResultSet resultSet = statement.executeQuery("count timeseries")) {
        Assert.assertTrue(resultSet.next());
        long resultRecord = resultSet.getLong(1);
        Assert.assertEquals(3, resultRecord);
      }
    }
  }

  @Test
  public void testUnsetTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      // show paths set device template
      String[] expectedResult = new String[] {"root.sg1.d1"};
      Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE t1")) {
        String resultRecord;
        while (resultSet.next()) {
          resultRecord = resultSet.getString(1);
          Assert.assertTrue(expectedResultSet.contains(resultRecord));
          expectedResultSet.remove(resultRecord);
        }
      }
      Assert.assertEquals(0, expectedResultSet.size());
      // unset device template
      statement.execute("UNSET DEVICE TEMPLATE t1 FROM root.sg1.d1");
      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE t1")) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testTemplateSetAndTimeSeriesExistenceCheck() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      // show paths set device template
      String[] expectedResult = new String[] {"root.sg1.d1"};
      Set<String> expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE t1")) {
        String resultRecord;
        while (resultSet.next()) {
          resultRecord = resultSet.getString(1);
          Assert.assertTrue(expectedResultSet.contains(resultRecord));
          expectedResultSet.remove(resultRecord);
        }
      }
      Assert.assertEquals(0, expectedResultSet.size());

      try {
        statement.execute("CREATE TIMESERIES root.sg1.d1.s INT32");
        fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            "516: Cannot create timeseries [root.sg1.d1.s] since device template [t1] already set on path [root.sg1.d1].",
            e.getMessage());
      }

      // unset device template
      statement.execute("UNSET DEVICE TEMPLATE t1 FROM root.sg1.d1");
      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE t1")) {
        Assert.assertFalse(resultSet.next());
      }

      statement.execute("CREATE TIMESERIES root.sg1.d1.s INT32");

      try {
        statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      } catch (SQLException e) {
        Assert.assertEquals(
            "516: Cannot set device template [t1] to path [root.sg1.d1] since there's timeseries under path [root.sg1.d1].",
            e.getMessage());
      }

      statement.execute("DELETE TIMESERIES root.sg1.d1.s");

      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      expectedResult = new String[] {"root.sg1.d1"};
      expectedResultSet = new HashSet<>(Arrays.asList(expectedResult));
      try (ResultSet resultSet = statement.executeQuery("SHOW PATHS SET DEVICE TEMPLATE t1")) {
        String resultRecord;
        while (resultSet.next()) {
          resultRecord = resultSet.getString(1);
          Assert.assertTrue(expectedResultSet.contains(resultRecord));
          expectedResultSet.remove(resultRecord);
        }
      }
      Assert.assertEquals(0, expectedResultSet.size());

      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d2.tmp.m");
      try {
        statement.execute("CREATE TIMESERIES root.sg1.d2 INT32");
      } catch (SQLException e) {
        Assert.assertEquals(
            "516: Cannot create timeseries [root.sg1.d2] since device template [t1] already set on path [root.sg1.d2.tmp.m].",
            e.getMessage());
      }
      try {
        statement.execute("CREATE TIMESERIES root.sg1.d2.s(tmp) INT32");
      } catch (SQLException e) {
        Assert.assertEquals(
            "516: Cannot create timeseries [root.sg1.d2.s] since device template [t1] already set on path [root.sg1.d2.tmp.m].",
            e.getMessage());
      }
      statement.execute("CREATE TIMESERIES root.sg1.d2.s INT32");
    }
  }

  @Test
  public void testShowTemplateSeriesWithFuzzyQuery() throws Exception {
    // test create device template repeatedly
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // set device template
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg2");
      statement.execute("SET DEVICE TEMPLATE t3 TO root.sg3");
      // activate device template
      statement.execute("create timeseries using device template on root.sg1.d1");
      statement.execute("create timeseries using device template on root.sg2.d2");
      statement.execute("create timeseries using device template on root.sg3.d3");

      Set<String> expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.sg1.d1.s1,INT64,TS_2DIFF,LZ4",
                  "root.sg1.d1.s2,DOUBLE,GORILLA,LZ4",
                  "root.sg2.d2.s1,INT64,TS_2DIFF,LZ4",
                  "root.sg2.d2.s2,DOUBLE,GORILLA,LZ4",
                  "root.sg3.d3.s1,INT64,TS_2DIFF,LZ4"));

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg*.*.s*")) {
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
      expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.sg1.d1.s1,INT64,TS_2DIFF,LZ4", "root.sg1.d1.s2,DOUBLE,GORILLA,LZ4"));

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg1.d1.s*")) {
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
    }
  }

  @Test
  public void testEmptySchemaTemplate() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create empty device template
      statement.execute("create device template e_t");
      // set device template
      statement.execute("SET DEVICE TEMPLATE e_t TO root.sg1");
      try (ResultSet resultSet = statement.executeQuery("show nodes in device template e_t")) {
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("show paths set device template e_t")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }

      statement.execute("alter device template e_t add(s1 int32)");
      statement.execute("insert into root.sg1.d(time, s2, s3) values(1, 1, 1)");

      Set<String> expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.sg1.d.s1,INT32,TS_2DIFF,LZ4",
                  "root.sg1.d.s2,DOUBLE,GORILLA,LZ4",
                  "root.sg1.d.s3,DOUBLE,GORILLA,LZ4"));

      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.sg*.*.s*")) {
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
    }
  }

  @Test
  public void testLevelCountWithTemplate() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1");
      statement.execute("SET DEVICE TEMPLATE t2 TO root.sg1.d2");
      statement.execute("SET DEVICE TEMPLATE t3 TO root.sg1.d3");
      // create timeseries of device template
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d1");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d2");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d3");
      // count
      Set<String> expectedResult =
          new HashSet<>(Arrays.asList("root.sg1.d1,2", "root.sg1.d2,2", "root.sg1.d3,1"));
      try (ResultSet resultSet =
          statement.executeQuery("COUNT TIMESERIES root.sg1.** group by level=2")) {
        while (resultSet.next()) {
          String actualResult =
              resultSet.getString(ColumnHeaderConstant.COLUMN)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COUNT_TIMESERIES);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());
    }
  }

  @Test
  public void testAlterTemplateTimeseries() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET DEVICE TEMPLATE t1 TO root.sg1.d1;");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.d1;");
      try {
        statement.execute(
            "ALTER timeseries root.sg1.d1.s1 UPSERT tags(s0_tag1=s0_tag1, s0_tag2=s0_tag2) attributes(s0_attr1=s0_attr1, s0_attr2=s0_attr2);");
        Assert.fail("expect exception because the template timeseries does not support tag");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "Cannot alter template timeseries [root.sg1.d1.s1] since device template [t1] already set on path [root.sg1.d1]"));
      }
      try {
        statement.execute("ALTER timeseries root.sg1.d1.s1 UPSERT ALIAS=s0Alias;");
        Assert.fail("expect exception because the template timeseries does not support alias");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "Cannot alter template timeseries [root.sg1.d1.s1] since device template [t1] already set on path [root.sg1.d1]"));
      }
    }
  }

  @Test
  public void testActivateAndDropEmptyTemplate() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DEVICE TEMPLATE e_t;");
      statement.execute("SET DEVICE TEMPLATE e_t TO root.sg1.t.d1;");
      statement.execute("insert into root.sg1.t.d2(timestamp,s1) values(now(),false);");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.sg1.t.d1;");
      try (ResultSet resultSet = statement.executeQuery("show nodes in device template e_t")) {
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("show paths set device template e_t")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
      statement.execute("DEACTIVATE DEVICE TEMPLATE FROM root.sg1.t.d1;");
      statement.execute("UNSET DEVICE TEMPLATE e_t FROM root.sg1.t.d1;");
      try (ResultSet resultSet = statement.executeQuery("show paths set device template e_t")) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }
}
