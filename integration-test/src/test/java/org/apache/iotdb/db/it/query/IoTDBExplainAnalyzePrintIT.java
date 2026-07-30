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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBExplainAnalyzePrintIT {

  private static final String MERGE_THRESHOLD_OF_EXPLAIN_ANALYZE =
      "merge_threshold_of_explain_analyze";
  private static final String SERIES_SCAN_OPERATOR = "SeriesScanOperator";
  private static final String FILTER_AND_PROJECT_OPERATOR = "FilterAndProjectOperator";
  private static final String FILTERED_ROWS = "Filtered Rows";

  private static final String[] creationSqls =
      new String[] {
        "insert into root.test.device_0(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10) values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("set configuration \"%s\"=\"2\"", MERGE_THRESHOLD_OF_EXPLAIN_ANALYZE));
      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testOperatorStatisticsWhenMergedInAnalyze() throws SQLException {
    String output =
        getExplainAnalyzeResult(
            "explain analyze select s1 from root.test.device_0 where s1 > 100 or s2 > 100");
    assertTrue(output, containsLine(output, SERIES_SCAN_OPERATOR, "Count: * 2"));
    assertTrue(output, output.contains(FILTER_AND_PROJECT_OPERATOR));
    assertTrue(output, output.contains(FILTERED_ROWS));
  }

  private static String getExplainAnalyzeResult(String sql) throws SQLException {
    StringBuilder output = new StringBuilder();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        output.append(resultSet.getString(1)).append('\n');
      }
    }
    return output.toString();
  }

  private static boolean containsLine(String output, String... expectedValues) {
    for (String line : output.split("\\R")) {
      boolean matched = true;
      for (String expectedValue : expectedValues) {
        if (!line.contains(expectedValue)) {
          matched = false;
          break;
        }
      }
      if (matched) {
        return true;
      }
    }
    return false;
  }
}
