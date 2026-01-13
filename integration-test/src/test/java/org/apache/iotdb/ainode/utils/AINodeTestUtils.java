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

package org.apache.iotdb.ainode.utils;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AINodeTestUtils {

  public static final Map<String, FakeModelInfo> BUILTIN_LTSM_MAP =
      Stream.of(
              new AbstractMap.SimpleEntry<>(
                  "sundial", new FakeModelInfo("sundial", "sundial", "builtin", "active")),
              new AbstractMap.SimpleEntry<>(
                  "timer_xl", new FakeModelInfo("timer_xl", "timer", "builtin", "active")),
              new AbstractMap.SimpleEntry<>(
                  "chronos2", new FakeModelInfo("chronos2", "t5", "builtin", "active")))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  public static final Map<String, FakeModelInfo> BUILTIN_MODEL_MAP;

  static {
    Map<String, FakeModelInfo> tmp =
        Stream.of(
                new AbstractMap.SimpleEntry<>(
                    "arima", new FakeModelInfo("arima", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "holtwinters", new FakeModelInfo("holtwinters", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "exponential_smoothing",
                    new FakeModelInfo("exponential_smoothing", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "naive_forecaster",
                    new FakeModelInfo("naive_forecaster", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "stl_forecaster",
                    new FakeModelInfo("stl_forecaster", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "gaussian_hmm",
                    new FakeModelInfo("gaussian_hmm", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "gmm_hmm", new FakeModelInfo("gmm_hmm", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "stray", new FakeModelInfo("stray", "sktime", "builtin", "active")))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    tmp.putAll(BUILTIN_LTSM_MAP);
    BUILTIN_MODEL_MAP = Collections.unmodifiableMap(tmp);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AINodeTestUtils.class);

  public static void checkHeader(ResultSetMetaData resultSetMetaData, String title)
      throws SQLException {
    String[] headers = title.split(",");
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(headers[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  public static void errorTest(Statement statement, String sql, String errorMessage) {
    try (ResultSet ignored = statement.executeQuery(sql)) {
      fail("There should be an exception");
    } catch (SQLException e) {
      assertEquals(errorMessage, e.getMessage());
    }
  }

  public static void concurrentInference(
      Statement statement, String sql, int threadCnt, int loop, int expectedOutputLength)
      throws InterruptedException {
    AtomicBoolean allPass = new AtomicBoolean(true);
    Thread[] threads = new Thread[threadCnt];
    for (int i = 0; i < threadCnt; i++) {
      threads[i] =
          new Thread(
              () -> {
                try {
                  for (int j = 0; j < loop; j++) {
                    try (ResultSet resultSet = statement.executeQuery(sql)) {
                      int outputCnt = 0;
                      while (resultSet.next()) {
                        outputCnt++;
                      }
                      if (expectedOutputLength != outputCnt) {
                        allPass.set(false);
                        fail(
                            "Output count mismatch for SQL: "
                                + sql
                                + ". Expected: "
                                + expectedOutputLength
                                + ", but got: "
                                + outputCnt);
                      }
                    } catch (SQLException e) {
                      allPass.set(false);
                      fail(e.getMessage());
                    }
                  }
                } catch (Exception e) {
                  allPass.set(false);
                  fail(e.getMessage());
                }
              });
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join(TimeUnit.MINUTES.toMillis(10));
      if (thread.isAlive()) {
        fail("Thread timeout after 10 minutes");
      }
    }
    Assert.assertTrue(allPass.get());
  }

  public static void checkModelOnSpecifiedDevice(Statement statement, String modelId, String device)
      throws SQLException, InterruptedException {
    Set<String> targetDevices = ImmutableSet.copyOf(device.split(","));
    LOGGER.info("Checking model: {} on target devices: {}", modelId, targetDevices);
    for (int retry = 0; retry < 200; retry++) {
      Set<String> foundDevices = new HashSet<>();
      try (final ResultSet resultSet =
          statement.executeQuery(String.format("SHOW LOADED MODELS '%s'", device))) {
        while (resultSet.next()) {
          String deviceId = resultSet.getString("DeviceId");
          String loadedModelId = resultSet.getString("ModelId");
          int count = resultSet.getInt("Count(instances)");
          LOGGER.info("Model {} found in device {}, count {}", loadedModelId, deviceId, count);
          if (loadedModelId.equals(modelId) && targetDevices.contains(deviceId) && count > 0) {
            foundDevices.add(deviceId);
            LOGGER.info("Model {} is loaded to device {}", modelId, device);
          }
        }
        if (foundDevices.containsAll(targetDevices)) {
          LOGGER.info("Model {} is loaded to devices {}, start testing", modelId, targetDevices);
          return;
        }
      }
      TimeUnit.SECONDS.sleep(3);
    }
    fail("Model " + modelId + " is not loaded on device " + device);
  }

  public static void checkModelNotOnSpecifiedDevice(
      Statement statement, String modelId, String device)
      throws SQLException, InterruptedException {
    Set<String> targetDevices = ImmutableSet.copyOf(device.split(","));
    LOGGER.info("Checking model: {} not on target devices: {}", modelId, targetDevices);
    for (int retry = 0; retry < 50; retry++) {
      Set<String> foundDevices = new HashSet<>();
      try (final ResultSet resultSet =
          statement.executeQuery(String.format("SHOW LOADED MODELS '%s'", device))) {
        while (resultSet.next()) {
          String deviceId = resultSet.getString("DeviceId");
          String loadedModelId = resultSet.getString("ModelId");
          int count = resultSet.getInt("Count(instances)");
          LOGGER.info("Model {} found in device {}, count {}", loadedModelId, deviceId, count);
          if (loadedModelId.equals(modelId) && targetDevices.contains(deviceId) && count > 0) {
            foundDevices.add(deviceId);
            LOGGER.info("Model {} is loaded to device {}", modelId, device);
          }
        }
        if (foundDevices.isEmpty()) {
          LOGGER.info("Model {} is unloaded from devices {}.", modelId, targetDevices);
          return;
        }
      }
      TimeUnit.SECONDS.sleep(3);
    }
    fail("Model " + modelId + " is still loaded on device " + device);
  }

  private static final String[] WRITE_SQL_IN_TREE =
      new String[] {
        "CREATE DATABASE root.AI",
        "CREATE TIMESERIES root.AI.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s1 WITH DATATYPE=DOUBLE, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s2 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s3 WITH DATATYPE=INT64, ENCODING=RLE",
      };

  /** Prepare root.AI(s0 FLOAT, s1 DOUBLE, s2 INT32, s3 INT64) with 5760 rows of data in tree. */
  public static void prepareDataInTree() throws SQLException {
    prepareData(WRITE_SQL_IN_TREE);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 5760; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.AI(timestamp,s0,s1,s2,s3) VALUES(%d,%f,%f,%d,%d)",
                i, (float) i, (double) i, i, i));
      }
    }
  }

  /** Prepare db.AI(s0 FLOAT, s1 DOUBLE, s2 INT32, s3 INT64) with 5760 rows of data in table. */
  public static void prepareDataInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE db");
      statement.execute(
          "CREATE TABLE db.AI (s0 FLOAT FIELD, s1 DOUBLE FIELD, s2 INT32 FIELD, s3 INT64 FIELD)");
      for (int i = 0; i < 5760; i++) {
        statement.execute(
            String.format(
                "INSERT INTO db.AI(time,s0,s1,s2,s3) VALUES(%d,%f,%f,%d,%d)",
                i, (float) i, (double) i, i, i));
      }
    }
  }

  public static class FakeModelInfo {

    private final String modelId;
    private final String modelType;
    private final String category;
    private final String state;

    public FakeModelInfo(String modelId, String modelType, String category, String state) {
      this.modelId = modelId;
      this.modelType = modelType;
      this.category = category;
      this.state = state;
    }

    public String getModelId() {
      return modelId;
    }

    public String getModelType() {
      return modelType;
    }

    public String getCategory() {
      return category;
    }

    public String getState() {
      return state;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FakeModelInfo modelInfo = (FakeModelInfo) o;
      return Objects.equals(modelId, modelInfo.modelId)
          && Objects.equals(modelType, modelInfo.modelType)
          && Objects.equals(category, modelInfo.category)
          && Objects.equals(state, modelInfo.state);
    }

    @Override
    public int hashCode() {
      return Objects.hash(modelId, modelType, category, state);
    }

    @Override
    public String toString() {
      return "FakeModelInfo{"
          + "modelId='"
          + modelId
          + '\''
          + ", modelType='"
          + modelType
          + '\''
          + ", category='"
          + category
          + '\''
          + ", state='"
          + state
          + '\''
          + '}';
    }
  }
}
