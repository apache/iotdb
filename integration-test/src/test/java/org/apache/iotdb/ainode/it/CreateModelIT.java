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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class CreateModelIT {
  static final String TIMERXL_MODEL_PATH =
      System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "timerxl-example";

  static final String SUNDIAL_MODEL_PATH =
      System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "sundial-example";

  // Legacy format paths for backward compatibility testing
  static final String LEGACY_MODEL_PATH =
      System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "legacy-example";

  static String[] setupSqls =
      new String[] {
        "set configuration \"trusted_uri_pattern\"='.*'",
        "CREATE DATABASE root.iotdb.test",
        "CREATE TIMESERIES root.iotdb.test.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.iotdb.test.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
      };

  static String[] timeSeriesData = new String[96];

  static {
    // Generate 96 time series data points
    for (int i = 0; i < 96; i++) {
      float value = (float) Math.sin(i * 0.1) + (float) Math.random() * 0.1f;
      timeSeriesData[i] =
          String.format(
              "insert into root.iotdb.test(timestamp,s0,s1) values(%d,%.3f,%.3f)",
              i + 1, value, value + 0.1f);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    // Prepare basic setup and data
    String[] allSqls = new String[setupSqls.length + timeSeriesData.length];
    System.arraycopy(setupSqls, 0, allSqls, 0, setupSqls.length);
    System.arraycopy(timeSeriesData, 0, allSqls, setupSqls.length, timeSeriesData.length);

    prepareData(allSqls);
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

  @Test
  public void timerXLModelOperationTest() {
    String registerSql = "create model timerxl_test using uri \"" + TIMERXL_MODEL_PATH + "\"";
    String showSql = "SHOW MODELS timerxl_test";
    String dropSql = "DROP MODEL timerxl_test";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Register model
      statement.execute(registerSql);

      // Wait for model to load
      boolean modelReady = false;
      int maxRetries = 30; // Wait up to 30 seconds

      for (int i = 0; i < maxRetries; i++) {
        try (ResultSet resultSet = statement.executeQuery(showSql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          checkHeader(resultSetMetaData, "ModelId,ModelType,State,Configs,Notes");

          while (resultSet.next()) {
            String modelName = resultSet.getString(1);
            String modelType = resultSet.getString(2);
            String status = resultSet.getString(3);

            assertEquals("timerxl_test", modelName);
            assertEquals("USER_DEFINED", modelType);

            if (status.equals("ACTIVE")) {
              modelReady = true;
              break;
            } else if (status.equals("LOADING")) {
              Thread.sleep(1000); // Wait 1 second
              break;
            } else {
              fail("Unexpected model status: " + status);
            }
          }
        }

        if (modelReady) break;
      }

      assertTrue("Model failed to become ACTIVE within timeout", modelReady);

      // Delete model
      statement.execute(dropSql);

      // Verify model is deleted
      try (ResultSet resultSet = statement.executeQuery(showSql)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

    } catch (SQLException | InterruptedException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void timerXLInferenceTest() {
    String registerSql = "create model timerxl_inference using uri \"" + TIMERXL_MODEL_PATH + "\"";
    String inferenceSql =
        "CALL INFERENCE(timerxl_inference, \"select s0 from root.iotdb.test\", generateTime=true)";
    String dropSql = "DROP MODEL timerxl_inference";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Register model
      statement.execute(registerSql);

      // Wait for model to be ready
      Thread.sleep(5000);

      // Execute inference
      try (ResultSet resultSet = statement.executeQuery(inferenceSql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        // Check output columns
        assertTrue(
            "Should have Time column", resultSetMetaData.getColumnName(1).equals("Time"));
        assertTrue(
            "Should have at least one output column", resultSetMetaData.getColumnCount() >= 2);

        // Check output data
        int rowCount = 0;
        while (resultSet.next()) {
          rowCount++;
          // Verify time column is not null
          assertNotNull("Time should not be null", resultSet.getTimestamp(1));

          // Verify output value is numeric
          float outputValue = resultSet.getFloat(2);
          assertTrue("Output should be a valid number", !Float.isNaN(outputValue));
        }

        assertTrue("Should have output rows", rowCount > 0);
        System.out.println("TimerXL inference generated " + rowCount + " predictions");
      }

      // Cleanup
      statement.execute(dropSql);

    } catch (SQLException | InterruptedException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void sundialModelOperationTest() {
    String registerSql = "create model sundial_test using uri \"" + SUNDIAL_MODEL_PATH + "\"";
    String showSql = "SHOW MODELS sundial_test";
    String dropSql = "DROP MODEL sundial_test";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Register model
      statement.execute(registerSql);

      // Wait for model to load
      boolean modelReady = false;
      int maxRetries = 30;

      for (int i = 0; i < maxRetries; i++) {
        try (ResultSet resultSet = statement.executeQuery(showSql)) {
          while (resultSet.next()) {
            String status = resultSet.getString(3);
            if (status.equals("ACTIVE")) {
              modelReady = true;
              break;
            } else if (status.equals("LOADING")) {
              Thread.sleep(1000);
              break;
            }
          }
        }
        if (modelReady) break;
      }

      assertTrue("Sundial model failed to become ACTIVE", modelReady);

      // Delete model
      statement.execute(dropSql);

    } catch (SQLException | InterruptedException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void legacyModelCompatibilityTest() {
    String registerSql = "create model legacy_test using uri \"" + LEGACY_MODEL_PATH + "\"";
    String showSql = "SHOW MODELS legacy_test";
    String dropSql = "DROP MODEL legacy_test";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Register legacy model
      statement.execute(registerSql);

      // Wait for model to load
      boolean modelReady = false;
      int maxRetries = 30;

      for (int i = 0; i < maxRetries; i++) {
        try (ResultSet resultSet = statement.executeQuery(showSql)) {
          while (resultSet.next()) {
            String status = resultSet.getString(3);
            if (status.equals("ACTIVE")) {
              modelReady = true;
              break;
            } else if (status.equals("LOADING")) {
              Thread.sleep(1000);
              break;
            }
          }
        }
        if (modelReady) break;
      }

      assertTrue("Legacy model failed to become ACTIVE", modelReady);

      // Delete model
      statement.execute(dropSql);

    } catch (SQLException | InterruptedException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void iotdbModelFormatDetectionTest() {
    // Test that IoTDB format (config.json + safetensors) is detected and used properly
    String registerSql = "create model format_test using uri \"" + TIMERXL_MODEL_PATH + "\"";
    String showSql = "SHOW MODELS format_test";
    String dropSql = "DROP MODEL format_test";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Register model
      statement.execute(registerSql);

      // Check that model loads successfully with IoTDB format
      boolean modelReady = false;
      int maxRetries = 30;

      for (int i = 0; i < maxRetries; i++) {
        try (ResultSet resultSet = statement.executeQuery(showSql)) {
          while (resultSet.next()) {
            String modelName = resultSet.getString(1);
            String status = resultSet.getString(3);
            
            assertEquals("format_test", modelName);
            
            if (status.equals("ACTIVE")) {
              modelReady = true;
              System.out.println("IoTDB format model loaded successfully");
              break;
            } else if (status.equals("LOADING")) {
              Thread.sleep(1000);
              break;
            }
          }
        }
        if (modelReady) break;
      }

      assertTrue("IoTDB format model failed to load", modelReady);

      // Cleanup
      statement.execute(dropSql);

    } catch (SQLException | InterruptedException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void iotdbModelErrorHandlingTest() {
    // Test invalid URI
    String invalidUriSql = "create model invalid_model using uri \"/nonexistent/path\"";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try {
        statement.execute(invalidUriSql);
        fail("Should throw exception for invalid URI");
      } catch (SQLException e) {
        assertTrue(
            "Should contain error message about invalid URI",
            e.getMessage().contains("invalid") || e.getMessage().contains("not found"));
      }

    } catch (SQLException e) {
      fail("Unexpected error: " + e.getMessage());
    }
  }

  private void assertNotNull(String message, Object value) {
    if (value == null) {
      fail(message);
    }
  }
}