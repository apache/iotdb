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
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
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

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class SetConfigurationCliIT {

  private static final String SUCCESS_MESSAGE = "Msg: The statement is executed successfully.";
  private static final String RESTART_REQUIRED_MESSAGE =
      SUCCESS_MESSAGE
          + "Configuration has been set successfully, but restart is required for the following"
          + " parameters to take effect: ";

  private static String ip;
  private static String port;
  private static String sbinPath;
  private static String homePath;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();
    sbinPath = EnvFactory.getEnv().getSbinPath();
    String libPath = EnvFactory.getEnv().getLibPath();
    homePath =
        libPath.substring(0, libPath.lastIndexOf(File.separator + "lib" + File.separator + "*"));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTableSetConfigurationCliMessage() throws Exception {
    assertCliMessage(
        BaseEnv.TABLE_SQL_DIALECT,
        "set configuration dn_rpc_max_concurrent_client_num='14', enable_black_list='false';",
        RESTART_REQUIRED_MESSAGE + "dn_rpc_max_concurrent_client_num.");
    assertCliMessage(
        BaseEnv.TABLE_SQL_DIALECT,
        "set configuration dn_rpc_max_concurrent_client_num='14'",
        RESTART_REQUIRED_MESSAGE + "dn_rpc_max_concurrent_client_num.");
    assertCliMessage(
        BaseEnv.TABLE_SQL_DIALECT, "set configuration enable_black_list='false';", SUCCESS_MESSAGE);
  }

  @Test
  public void testTreeSetConfigurationCliMessage() throws Exception {
    assertCliMessage(
        BaseEnv.TREE_SQL_DIALECT,
        "set configuration 'dn_rpc_max_concurrent_client_num'='14' 'enable_mqtt_service'='false';",
        RESTART_REQUIRED_MESSAGE + "enable_mqtt_service, dn_rpc_max_concurrent_client_num.");
    assertCliMessage(
        BaseEnv.TREE_SQL_DIALECT,
        "set configuration 'dn_rpc_max_concurrent_client_num'='14';",
        RESTART_REQUIRED_MESSAGE + "dn_rpc_max_concurrent_client_num.");
    assertCliMessage(
        BaseEnv.TREE_SQL_DIALECT,
        "set configuration 'enable_black_list'='false';",
        SUCCESS_MESSAGE);
  }

  private void assertCliMessage(String sqlDialect, String sql, String expectedMessage)
      throws IOException, InterruptedException {
    CliResult cliResult = executeCliCommand(sqlDialect, sql);
    Assert.assertEquals(cliResult.output, 0, cliResult.exitCode);
    Assert.assertTrue(
        cliResult.output,
        cliResult.outputLines.stream().anyMatch(line -> expectedMessage.equals(line.trim())));
  }

  private CliResult executeCliCommand(String sqlDialect, String sql)
      throws IOException, InterruptedException {
    ProcessBuilder builder;
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      builder =
          new ProcessBuilder(
              "cmd.exe",
              "/c",
              sbinPath + File.separator + "windows" + File.separator + "start-cli.bat",
              "-h",
              ip,
              "-p",
              port,
              "-u",
              "root",
              "-pw",
              "TimechoDB@2021",
              "-sql_dialect",
              sqlDialect,
              "-e",
              sql,
              "&",
              "exit",
              "%^errorlevel%");
    } else {
      builder =
          new ProcessBuilder(
              "bash",
              sbinPath + File.separator + "start-cli.sh",
              "-h",
              ip,
              "-p",
              port,
              "-u",
              "root",
              "-pw",
              "TimechoDB@2021",
              "-sql_dialect",
              sqlDialect,
              "-e",
              "\"" + sql + "\"");
    }
    builder.environment().put("IOTDB_HOME", homePath);
    builder.redirectErrorStream(true);

    Process process = builder.start();
    List<String> outputLines = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        outputLines.add(line);
      }
    }

    int exitCode = process.waitFor();
    return new CliResult(exitCode, outputLines);
  }

  private static class CliResult {
    private final int exitCode;
    private final List<String> outputLines;
    private final String output;

    private CliResult(int exitCode, List<String> outputLines) {
      this.exitCode = exitCode;
      this.outputLines = outputLines;
      this.output = String.join(System.lineSeparator(), outputLines);
    }
  }
}
