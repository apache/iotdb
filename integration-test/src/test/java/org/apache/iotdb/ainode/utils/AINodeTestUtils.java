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

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AINodeTestUtils {

  public static final String EXAMPLE_MODEL_PATH =
      "file://"
          + System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "ainode-example";

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
                      assertEquals(expectedOutputLength, outputCnt);
                    } catch (SQLException e) {
                      fail(e.getMessage());
                    }
                  }
                } catch (Exception e) {
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
