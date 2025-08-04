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
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class StartClientScriptIT extends AbstractScriptIT {

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

    final String[] output = {
      "Error: Connection Error, please check whether the network is available or the server has started."
    };
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "windows" + File.separator + "start-cli.bat",
            "-h",
            ip,
            "-p",
            "6668",
            "-u",
            "root",
            "-pw",
            "root",
            "&",
            "exit",
            "%^errorlevel%");
    builder.environment().put("IOTDB_HOME", homePath);
    testOutput(builder, output, 1);

    final String[] output2 = {"Msg: The statement is executed successfully."};
    ProcessBuilder builder2 =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "windows" + File.separator + "start-cli.bat",
            "-h",
            ip,
            "-p",
            port,
            "-e",
            "\"flush\"",
            "&",
            "exit",
            "%^errorlevel%");
    builder2.environment().put("IOTDB_HOME", homePath);
    testOutput(builder2, output2, 0);
  }

  @Override
  protected void testOnUnix() throws IOException {

    final String[] output = {
      "Error: Connection Error, please check whether the network is available or the server has started."
    };
    ProcessBuilder builder =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            "6668",
            "-u",
            "root",
            "-pw",
            "root");
    builder.environment().put("IOTDB_HOME", homePath);
    testOutput(builder, output, 1);

    final String[] output2 = {"Msg: The statement is executed successfully."};
    ProcessBuilder builder2 =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-e",
            "\"flush\"");
    builder2.environment().put("IOTDB_HOME", homePath);
    testOutput(builder2, output2, 0);

    // test null display
    final String[] successfulDisplay = {"Msg: The statement is executed successfully."};
    ProcessBuilder builder3 =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-e",
            "\"CREATE ALIGNED TIMESERIES root.db.d1(s_boolean BOOLEAN, s_int32 INT32)\"");
    builder3.environment().put("IOTDB_HOME", homePath);
    testOutput(builder3, successfulDisplay, 0);

    ProcessBuilder builder4 =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-e",
            "\"insert into root.db.d1(time, s_int32) values(0,0)\"");
    builder4.environment().put("IOTDB_HOME", homePath);
    testOutput(builder4, successfulDisplay, 0);

    final String[] output5 = {"Time zone has set to asia/shanghai"};
    ProcessBuilder builder5 =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-e",
            "\"set time_zone=Asia/Shanghai\"");
    builder5.environment().put("IOTDB_HOME", homePath);
    testOutput(builder5, output5, 0);

    final String[] output6 = {
      "+----+------------------+--------------------+",
      "|Time|root.db.d1.s_int32|root.db.d1.s_boolean|",
      "+----+------------------+--------------------+",
      "|   0|                 0|                null|",
      "+----+------------------+--------------------+",
      "Total line number = 1",
      "It costs "
    };
    ProcessBuilder builder6 =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-disableISO8601",
            "-e",
            "\"select s_int32, s_boolean from root.db.d1\"");
    builder6.environment().put("IOTDB_HOME", homePath);
    testOutput(builder6, output6, 0);
  }
}
