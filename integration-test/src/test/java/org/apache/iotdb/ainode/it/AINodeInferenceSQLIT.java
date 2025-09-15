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
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.EXAMPLE_MODEL_PATH;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeInferenceSQLIT {

  static String[] WRITE_SQL_IN_TREE =
      new String[] {
        "set configuration \"trusted_uri_pattern\"='.*'",
        "create model identity using uri \"" + EXAMPLE_MODEL_PATH + "\"",
        "CREATE DATABASE root.AI",
        "CREATE TIMESERIES root.AI.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s1 WITH DATATYPE=DOUBLE, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s2 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s3 WITH DATATYPE=INT64, ENCODING=RLE",
      };

  static String[] WRITE_SQL_IN_TABLE =
      new String[] {
        "CREATE DATABASE root",
        "CREATE TABLE root.AI (s0 FLOAT FIELD, s1 DOUBLE FIELD, s2 INT32 FIELD, s3 INT64 FIELD)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareData(WRITE_SQL_IN_TREE);
    prepareTableData(WRITE_SQL_IN_TABLE);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 2880; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.AI(timestamp,s0,s1,s2,s3) VALUES(%d,%f,%f,%d,%d)",
                i, (float) i, (double) i, i, i));
      }
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 2880; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.AI(time,s0,s1,s2,s3) VALUES(%d,%f,%f,%d,%d)",
                i, (float) i, (double) i, i, i));
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void callInferenceTestInTree() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      callInferenceTest(statement);
    }
  }

  // TODO: Enable this test after the call inference is supported by the table model
  //  @Test
  public void callInferenceTestInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      callInferenceTest(statement);
    }
  }

  public void callInferenceTest(Statement statement) throws SQLException {
    // SQL0: Invoke timer-sundial and timer-xl to inference, the result should success
    try (ResultSet resultSet =
        statement.executeQuery(
            "CALL INFERENCE(sundial, \"select s1 from root.AI\", generateTime=true, predict_length=720)")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "Time,output0");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(720, count);
    }
    try (ResultSet resultSet =
        statement.executeQuery(
            "CALL INFERENCE(timer_xl, \"select s2 from root.AI\", generateTime=true, predict_length=256)")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "Time,output0");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(256, count);
    }
    // SQL1: user-defined model inferences multi-columns with generateTime=true
    String sql1 =
        "CALL INFERENCE(identity, \"select s0,s1,s2,s3 from root.AI\", generateTime=true)";
    // SQL2: user-defined model inferences multi-columns with generateTime=false
    String sql2 =
        "CALL INFERENCE(identity, \"select s2,s0,s3,s1 from root.AI\", generateTime=false)";
    // SQL3: built-in model inferences single column with given predict_length and multi-outputs
    String sql3 =
        "CALL INFERENCE(naive_forecaster, \"select s0 from root.AI\", predict_length=3, generateTime=true)";
    // SQL4: built-in model inferences single column with given predict_length
    String sql4 =
        "CALL INFERENCE(holtwinters, \"select s0 from root.AI\", predict_length=6, generateTime=true)";
    // TODO: enable following tests after refactor the CALL INFERENCE

    //    try (ResultSet resultSet = statement.executeQuery(sql1)) {
    //      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    //      checkHeader(resultSetMetaData, "Time,output0,output1,output2,output3");
    //      int count = 0;
    //      while (resultSet.next()) {
    //        float s0 = resultSet.getFloat(2);
    //        float s1 = resultSet.getFloat(3);
    //        float s2 = resultSet.getFloat(4);
    //        float s3 = resultSet.getFloat(5);
    //
    //        assertEquals(s0, count + 1.0, 0.0001);
    //        assertEquals(s1, count + 2.0, 0.0001);
    //        assertEquals(s2, count + 3.0, 0.0001);
    //        assertEquals(s3, count + 4.0, 0.0001);
    //        count++;
    //      }
    //      assertEquals(7, count);
    //    }
    //
    //    try (ResultSet resultSet = statement.executeQuery(sql2)) {
    //      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    //      checkHeader(resultSetMetaData, "output0,output1,output2");
    //      int count = 0;
    //      while (resultSet.next()) {
    //        float s2 = resultSet.getFloat(1);
    //        float s0 = resultSet.getFloat(2);
    //        float s3 = resultSet.getFloat(3);
    //        float s1 = resultSet.getFloat(4);
    //
    //        assertEquals(s0, count + 1.0, 0.0001);
    //        assertEquals(s1, count + 2.0, 0.0001);
    //        assertEquals(s2, count + 3.0, 0.0001);
    //        assertEquals(s3, count + 4.0, 0.0001);
    //        count++;
    //      }
    //      assertEquals(7, count);
    //    }

    //    try (ResultSet resultSet = statement.executeQuery(sql3)) {
    //      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    //      checkHeader(resultSetMetaData, "Time,output0,output1,output2");
    //      int count = 0;
    //      while (resultSet.next()) {
    //        count++;
    //      }
    //      assertEquals(3, count);
    //    }

    //    try (ResultSet resultSet = statement.executeQuery(sql4)) {
    //      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    //      checkHeader(resultSetMetaData, "Time,output0");
    //      int count = 0;
    //      while (resultSet.next()) {
    //        count++;
    //      }
    //      assertEquals(6, count);
    //    }
  }

  @Test
  public void errorCallInferenceTestInTree() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      errorCallInferenceTest(statement);
    }
  }

  // TODO: Enable this test after the call inference is supported by the table model
  //  @Test
  public void errorCallInferenceTestInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      errorCallInferenceTest(statement);
    }
  }

  public void errorCallInferenceTest(Statement statement) {
    String sql = "CALL INFERENCE(notFound404, \"select s0,s1,s2 from root.AI\", window=head(5))";
    errorTest(statement, sql, "1505: model [notFound404] has not been created.");
    sql = "CALL INFERENCE(identity, \"select s0,s1,s2,s3 from root.AI\", window=head(2))";
    // TODO: enable following tests after refactor the CALL INFERENCE
    //    errorTest(statement, sql, "701: Window output 2 is not equal to input size of model 7");
    sql = "CALL INFERENCE(identity, \"select s0,s1,s2,s3 from root.AI limit 5\")";
    //    errorTest(
    //        statement,
    //        sql,
    //        "301: The number of rows 5 in the input data does not match the model input 7. Try to
    // use LIMIT in SQL or WINDOW in CALL INFERENCE");
    sql = "CREATE MODEL 中文 USING URI \"" + EXAMPLE_MODEL_PATH + "\"";
    errorTest(statement, sql, "701: ModelId can only contain letters, numbers, and underscores");
  }

  @Test
  public void selectForecastTestInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      // SQL0: Invoke timer-sundial and timer-xl to forecast, the result should success
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT * FROM FORECAST(model_id=>'sundial', input=>(SELECT time,s1 FROM root.AI) ORDER BY time, output_length=>720)")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "time,s1");
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(720, count);
      }
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT * FROM FORECAST(model_id=>'timer_xl', input=>(SELECT time,s2 FROM root.AI) ORDER BY time, output_length=>256)")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "time,s2");
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(256, count);
      }
      // SQL1: user-defined model inferences multi-columns with generateTime=true
      String sql1 =
          "SELECT * FROM FORECAST(model_id=>'identity', input=>(SELECT time,s0,s1,s2,s3 FROM root.AI) ORDER BY time, output_length=>7)";
      // SQL2: user-defined model inferences multi-columns with generateTime=false
      String sql2 =
          "SELECT * FROM FORECAST(model_id=>'identity', input=>(SELECT time,s2,s0,s3,s1 FROM root.AI) ORDER BY time, output_length=>7)";
      // SQL3: built-in model inferences single column with given predict_length and multi-outputs
      String sql3 =
          "SELECT * FROM FORECAST(model_id=>'naive_forecaster', input=>(SELECT time,s0 FROM root.AI) ORDER BY time, output_length=>3)";
      // SQL4: built-in model inferences single column with given predict_length
      String sql4 =
          "SELECT * FROM FORECAST(model_id=>'holtwinters', input=>(SELECT time,s0 FROM root.AI) ORDER BY time, output_length=>6)";
      // TODO: enable following tests after refactor the FORECAST
      //      try (ResultSet resultSet = statement.executeQuery(sql1)) {
      //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      //        checkHeader(resultSetMetaData, "time,s0,s1,s2,s3");
      //        int count = 0;
      //        while (resultSet.next()) {
      //          float s0 = resultSet.getFloat(2);
      //          float s1 = resultSet.getFloat(3);
      //          float s2 = resultSet.getFloat(4);
      //          float s3 = resultSet.getFloat(5);
      //
      //          assertEquals(s0, count + 1.0, 0.0001);
      //          assertEquals(s1, count + 2.0, 0.0001);
      //          assertEquals(s2, count + 3.0, 0.0001);
      //          assertEquals(s3, count + 4.0, 0.0001);
      //          count++;
      //        }
      //        assertEquals(7, count);
      //      }
      //
      //      try (ResultSet resultSet = statement.executeQuery(sql2)) {
      //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      //        checkHeader(resultSetMetaData, "time,s2,s0,s3,s1");
      //        int count = 0;
      //        while (resultSet.next()) {
      //          float s2 = resultSet.getFloat(1);
      //          float s0 = resultSet.getFloat(2);
      //          float s3 = resultSet.getFloat(3);
      //          float s1 = resultSet.getFloat(4);
      //
      //          assertEquals(s0, count + 1.0, 0.0001);
      //          assertEquals(s1, count + 2.0, 0.0001);
      //          assertEquals(s2, count + 3.0, 0.0001);
      //          assertEquals(s3, count + 4.0, 0.0001);
      //          count++;
      //        }
      //        assertEquals(7, count);
      //      }

      //      try (ResultSet resultSet = statement.executeQuery(sql3)) {
      //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      //        checkHeader(resultSetMetaData, "time,s0,s1,s2");
      //        int count = 0;
      //        while (resultSet.next()) {
      //          count++;
      //        }
      //        assertEquals(3, count);
      //      }

      //      try (ResultSet resultSet = statement.executeQuery(sql4)) {
      //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      //        checkHeader(resultSetMetaData, "time,s0");
      //        int count = 0;
      //        while (resultSet.next()) {
      //          count++;
      //        }
      //        assertEquals(6, count);
      //      }
    }
  }
}
