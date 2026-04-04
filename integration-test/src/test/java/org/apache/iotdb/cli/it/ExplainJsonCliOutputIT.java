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

package org.apache.iotdb.cli.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import com.google.gson.JsonParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that EXPLAIN (FORMAT JSON) and EXPLAIN ANALYZE (FORMAT JSON) output raw JSON in CLI without
 * table borders (no '|' or '+---' formatting), so users can directly copy the JSON for
 * visualization.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class ExplainJsonCliOutputIT extends AbstractScriptIT {

  private static String ip;
  private static String port;
  private static String sbinPath;
  private static String libPath;
  private static String homePath;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();
    sbinPath = EnvFactory.getEnv().getSbinPath();
    libPath = EnvFactory.getEnv().getLibPath();
    homePath =
        libPath.substring(0, libPath.lastIndexOf(File.separator + "lib" + File.separator + "*"));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws IOException {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
  }

  @Override
  protected void testOnWindows() throws IOException {
    // Setup test data
    ProcessBuilder setupBuilder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "windows" + File.separator + "start-cli.bat",
            "-h",
            ip,
            "-p",
            port,
            "-sql_dialect",
            "table",
            "-e",
            "\"CREATE DATABASE IF NOT EXISTS test_cli_json;"
                + " USE test_cli_json;"
                + " CREATE TABLE IF NOT EXISTS t1(id STRING TAG, v FLOAT FIELD);"
                + " INSERT INTO t1 VALUES(1000, 'd1', 1.0)\"",
            "&",
            "exit",
            "%^errorlevel%");
    setupBuilder.environment().put("IOTDB_HOME", homePath);
    testOutput(setupBuilder, new String[] {"Msg: The statement is executed successfully."}, 0);

    // Test EXPLAIN (FORMAT JSON) output has no table borders
    ProcessBuilder explainBuilder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "windows" + File.separator + "start-cli.bat",
            "-h",
            ip,
            "-p",
            port,
            "-sql_dialect",
            "table",
            "-e",
            "\"USE test_cli_json; EXPLAIN (FORMAT JSON) SELECT * FROM t1\"",
            "&",
            "exit",
            "%^errorlevel%");
    explainBuilder.environment().put("IOTDB_HOME", homePath);
    assertRawJsonOutput(explainBuilder, "distribution plan");

    // Test EXPLAIN ANALYZE (FORMAT JSON) output has no table borders
    ProcessBuilder analyzeBuilder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "windows" + File.separator + "start-cli.bat",
            "-h",
            ip,
            "-p",
            port,
            "-sql_dialect",
            "table",
            "-e",
            "\"USE test_cli_json; EXPLAIN ANALYZE (FORMAT JSON) SELECT * FROM t1\"",
            "&",
            "exit",
            "%^errorlevel%");
    analyzeBuilder.environment().put("IOTDB_HOME", homePath);
    assertRawJsonOutput(analyzeBuilder, "Explain Analyze");
  }

  @Override
  protected void testOnUnix() throws IOException {
    // Setup test data
    ProcessBuilder setupBuilder =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-sql_dialect",
            "table",
            "-e",
            "\"CREATE DATABASE IF NOT EXISTS test_cli_json;"
                + " USE test_cli_json;"
                + " CREATE TABLE IF NOT EXISTS t1(id STRING TAG, v FLOAT FIELD);"
                + " INSERT INTO t1 VALUES(1000, 'd1', 1.0)\"");
    setupBuilder.environment().put("IOTDB_HOME", homePath);
    testOutput(setupBuilder, new String[] {"Msg: The statement is executed successfully."}, 0);

    // Test EXPLAIN (FORMAT JSON) output has no table borders
    ProcessBuilder explainBuilder =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-sql_dialect",
            "table",
            "-e",
            "\"USE test_cli_json; EXPLAIN (FORMAT JSON) SELECT * FROM t1\"");
    explainBuilder.environment().put("IOTDB_HOME", homePath);
    assertRawJsonOutput(explainBuilder, "distribution plan");

    // Test EXPLAIN ANALYZE (FORMAT JSON) output has no table borders
    ProcessBuilder analyzeBuilder =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-sql_dialect",
            "table",
            "-e",
            "\"USE test_cli_json; EXPLAIN ANALYZE (FORMAT JSON) SELECT * FROM t1\"");
    analyzeBuilder.environment().put("IOTDB_HOME", homePath);
    assertRawJsonOutput(analyzeBuilder, "Explain Analyze");
  }

  /**
   * Collects all output lines from a process, then verifies: 1. The column header is printed before
   * JSON content 2. No JSON content line has table border characters ('|' prefix or '+---'
   * separator) 3. The combined JSON content is valid JSON
   */
  private void assertRawJsonOutput(ProcessBuilder builder, String expectedHeader)
      throws IOException {
    builder.redirectErrorStream(true);
    Process p = builder.start();
    BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    List<String> outputList = new ArrayList<>();
    while (true) {
      line = r.readLine();
      if (line == null) {
        break;
      } else {
        outputList.add(line);
      }
    }
    r.close();
    p.destroy();

    System.out.println("Process output:");
    for (String s : outputList) {
      System.out.println(s);
    }

    // Find the JSON content region: from the first line starting with '{' to the corresponding '}'
    int jsonStart = -1;
    int jsonEnd = -1;
    for (int i = 0; i < outputList.size(); i++) {
      String trimmed = outputList.get(i).trim();
      if (jsonStart == -1 && trimmed.startsWith("{")) {
        jsonStart = i;
      }
      // The last line that is just '}' marks the end of JSON
      if (jsonStart != -1 && trimmed.equals("}")) {
        jsonEnd = i;
      }
    }

    assertTrue("Should find JSON start '{'", jsonStart >= 0);
    assertTrue("Should find JSON end '}'", jsonEnd >= jsonStart);

    // Verify the column header with table border appears before JSON content
    // Expected format: +---+  |header|  +---+  {json...}  +---+
    assertTrue("Header border should appear before JSON content", jsonStart >= 3);
    assertTrue(
        "Header border line should be present",
        outputList.get(jsonStart - 3).trim().matches("\\+[-+]+\\+"));
    assertTrue(
        "Column header '" + expectedHeader + "' should be present",
        outputList.get(jsonStart - 2).contains(expectedHeader));
    assertTrue(
        "Header border line should be present",
        outputList.get(jsonStart - 1).trim().matches("\\+[-+]+\\+"));

    // Verify JSON content lines do not have '|' borders
    for (int i = jsonStart; i <= jsonEnd; i++) {
      String s = outputList.get(i).trim();
      assertFalse("JSON line should not start with '|', but found: " + s, s.startsWith("|"));
    }

    // Concatenate JSON lines and verify it's valid JSON
    StringBuilder jsonBuilder = new StringBuilder();
    for (int i = jsonStart; i <= jsonEnd; i++) {
      jsonBuilder.append(outputList.get(i));
    }
    String jsonStr = jsonBuilder.toString();
    try {
      JsonParser.parseString(jsonStr).getAsJsonObject();
    } catch (Exception e) {
      fail("Output should be valid JSON, but got parse error: " + e.getMessage());
    }

    // Verify process exit code
    while (p.isAlive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail();
      }
    }
    assertEquals(0, p.exitValue());
  }
}
