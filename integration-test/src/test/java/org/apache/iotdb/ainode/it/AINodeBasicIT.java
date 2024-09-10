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

package org.apache.iotdb.ainode.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.AIClusterIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeBasicIT {
  static final String MODEL_PATH =
      System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "ainode-example";

  static String[] sqls =
      new String[] {
        "create model identity using uri \"" + MODEL_PATH + "\"",
        "CREATE DATABASE root.AI.data",
        "CREATE TIMESERIES root.AI.data.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.data.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.data.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.data.s3 WITH DATATYPE=DOUBLE, ENCODING=RLE",
        "insert into root.AI.data(timestamp,s0,s1,s2,s3) values(1,1.0,2.0,3.0,4.0)",
        "insert into root.AI.data(timestamp,s0,s1,s2,s3) values(2,2.0,3.0,4.0,5.0)",
        "insert into root.AI.data(timestamp,s0,s1,s2,s3) values(3,3.0,4.0,5.0,6.0)",
        "insert into root.AI.data(timestamp,s0,s1,s2,s3) values(4,4.0,5.0,6.0,7.0)",
        "insert into root.AI.data(timestamp,s0,s1,s2,s3) values(5,5.0,6.0,7.0,8.0)",
        "insert into root.AI.data(timestamp,s0,s1,s2,s3) values(6,6.0,7.0,8.0,9.0)",
        "insert into root.AI.data(timestamp,s0,s1,s2,s3) values(7,7.0,8.0,9.0,10.0)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1M cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareData(sqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void checkHeader(ResultSetMetaData resultSetMetaData, String title)
      throws SQLException {
    String[] headers = title.split(",");
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(headers[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  private void errorTest(String sql, String errorMessage) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet ignored = statement.executeQuery(sql)) {
        fail("There should be an exception");
      }
    } catch (SQLException e) {
      assertEquals(errorMessage, e.getMessage());
    }
  }

  @Test
  public void aiNodeConnectionTest() {
    String sql = "SHOW AINODES";
    String title = "NodeID,Status,RpcAddress,RpcPort";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, title);
        int count = 0;
        while (resultSet.next()) {
          assertEquals("2", resultSet.getString(1));
          assertEquals("Running", resultSet.getString(2));
          count++;
        }
        assertEquals(1, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void ModelOperationTest() {
    String registerSql = "create model operationTest using uri \"" + MODEL_PATH + "\"";
    String showSql = "SHOW MODELS operationTest";
    String dropSql = "DROP MODEL operationTest";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(registerSql);
      try (ResultSet resultSet = statement.executeQuery(showSql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "ModelId,ModelType,State,Configs,Notes");
        int count = 0;
        while (resultSet.next()) {
          String modelName = resultSet.getString(1);
          String modelType = resultSet.getString(2);
          String status = resultSet.getString(3);

          assertEquals("operationTest", modelName);
          assertEquals("USER_DEFINED", modelType);
          assertEquals("ACTIVE", status);
          count++;
        }
        assertEquals(1, count);
      }
      statement.execute(dropSql);
      try (ResultSet resultSet = statement.executeQuery(showSql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "ModelId,ModelType,State,Configs,Notes");
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void callInferenceTest() {
    String sql = "CALL INFERENCE(identity, \"select s0,s1,s2 from root.AI.data\")";
    String sql2 = "CALL INFERENCE(identity, \"select s2,s0,s1 from root.AI.data\")";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "output0,output1,output2");
        int count = 0;
        while (resultSet.next()) {
          float s0 = resultSet.getFloat(1);
          float s1 = resultSet.getFloat(2);
          float s2 = resultSet.getFloat(3);

          assertEquals(s0, count + 1.0, 0.0001);
          assertEquals(s1, count + 2.0, 0.0001);
          assertEquals(s2, count + 3.0, 0.0001);
          count++;
        }
        assertEquals(7, count);
      }

      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "output0,output1,output2");
        int count = 0;
        while (resultSet.next()) {
          float s2 = resultSet.getFloat(1);
          float s0 = resultSet.getFloat(2);
          float s1 = resultSet.getFloat(3);

          assertEquals(s0, count + 1.0, 0.0001);
          assertEquals(s1, count + 2.0, 0.0001);
          assertEquals(s2, count + 3.0, 0.0001);
          count++;
        }
        assertEquals(7, count);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void errorTest() {
    String sql =
        "CALL INFERENCE(notFound404, \"select s0,s1,s2 from root.AI.data\", window=head(5))";
    errorTest(sql, "1505: model [notFound404] has not been created.");
    sql = "CALL INFERENCE(identity, \"select s0,s1,s2 from root.AI.data\", window=head(2))";
    errorTest(sql, "701: Window output 2 is not equal to input size of model 7");
    sql = "CALL INFERENCE(identity, \"select s0,s1,s2 from root.AI.data limit 5\")";
    errorTest(
        sql,
        "301: The number of rows 5 in the input data does not match the model input 7. Try to use LIMIT in SQL or WINDOW in CALL INFERENCE");
    sql = "CREATE MODEL 中文 USING URI \"" + MODEL_PATH + "\"";
    errorTest(sql, "701: ModelName can only contain letters, numbers, and underscores");
  }
}
