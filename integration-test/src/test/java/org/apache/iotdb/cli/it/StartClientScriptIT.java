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
public class StartClientScriptIT extends AbstractScript {

  private static String ip;

  private static String port;

  private static String sbinPath;

  private static String libPath;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();
    sbinPath = EnvFactory.getEnv().getSbinPath();
    libPath = EnvFactory.getEnv().getLibPath();
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
      "Error: Connection Error, please check whether the network is available or the server has started. Host is 127.0.0.1, port is 6668."
    };
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "start-cli.bat",
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
    builder.environment().put("CLASSPATH", libPath);
    testOutput(builder, output, 1);

    final String[] output2 = {"Msg: The statement is executed successfully."};
    ProcessBuilder builder2 =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "start-cli.bat",
            "-h",
            ip,
            "-p",
            port,
            "-maxPRC",
            "0",
            "-e",
            "\"flush\"",
            "&",
            "exit",
            "%^errorlevel%");
    builder2.environment().put("CLASSPATH", libPath);
    testOutput(builder2, output2, 0);

    final String[] output3 = {
      "Error: error format of max print row count, it should be an integer number"
    };
    ProcessBuilder builder3 =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "start-cli.bat",
            "-maxPRC",
            "-1111111111111111111111111111",
            "&",
            "exit",
            "%^errorlevel%");
    builder3.environment().put("CLASSPATH", libPath);
    testOutput(builder3, output3, 1);
  }

  @Override
  protected void testOnUnix() throws IOException {
    final String[] output = {
      "Error: Connection Error, please check whether the network is available or the server has started. Host is 127.0.0.1, port is 6668."
    };
    ProcessBuilder builder =
        new ProcessBuilder(
            "sh",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            "6668",
            "-u",
            "root",
            "-pw",
            "root");
    builder.environment().put("CLASSPATH", libPath);
    testOutput(builder, output, 1);

    final String[] output2 = {"Msg: The statement is executed successfully."};
    ProcessBuilder builder2 =
        new ProcessBuilder(
            "sh",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-maxPRC",
            "0",
            "-e",
            "\"flush\"");
    builder2.environment().put("CLASSPATH", libPath);
    testOutput(builder2, output2, 0);

    final String[] output3 = {
      "Error: error format of max print row count, it should be an integer number"
    };
    ProcessBuilder builder3 =
        new ProcessBuilder(
            "sh",
            sbinPath + File.separator + "start-cli.sh",
            "-maxPRC",
            "-1111111111111111111111111111");
    builder3.environment().put("CLASSPATH", libPath);
    testOutput(builder3, output3, 1);
  }
}
