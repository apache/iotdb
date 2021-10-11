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
package org.apache.iotdb.cli;

import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.*;

import java.io.File;
import java.io.IOException;

public class StartClientScriptIT extends AbstractScript {

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
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
    String dir = getCliPath();
    final String[] output = {
      "IoTDB> Connection Error, please check whether the network is available or the server has started. Host is 127.0.0.1, port is 6668."
    };
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            dir + File.separator + "sbin" + File.separator + "start-cli.bat",
            "-h",
            "127.0.0.1",
            "-p",
            "6668",
            "-u",
            "root",
            "-pw",
            "root");
    testOutput(builder, output);

    final String[] output2 = {"Msg: The statement is executed successfully."};
    ProcessBuilder builder2 =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            dir + File.separator + "sbin" + File.separator + "start-cli.bat",
            "-maxPRC",
            "0",
            "-e",
            "\"flush\"");
    testOutput(builder2, output2);

    final String[] output3 = {
      "IoTDB> error format of max print row count, it should be an integer number"
    };
    ProcessBuilder builder3 =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            dir + File.separator + "sbin" + File.separator + "start-cli.bat",
            "-maxPRC",
            "-1111111111111111111111111111");
    testOutput(builder3, output3);
  }

  @Override
  protected void testOnUnix() throws IOException {
    String dir = getCliPath();
    final String[] output = {
      "IoTDB> Connection Error, please check whether the network is available or the server has started. Host is 127.0.0.1, port is 6668."
    };
    ProcessBuilder builder =
        new ProcessBuilder(
            "sh",
            dir + File.separator + "sbin" + File.separator + "start-cli.sh",
            "-h",
            "127.0.0.1",
            "-p",
            "6668",
            "-u",
            "root",
            "-pw",
            "root");
    testOutput(builder, output);

    final String[] output2 = {"Msg: The statement is executed successfully."};
    ProcessBuilder builder2 =
        new ProcessBuilder(
            "sh",
            dir + File.separator + "sbin" + File.separator + "start-cli.sh",
            "-maxPRC",
            "0",
            "-e",
            "\"flush\"");
    testOutput(builder2, output2);

    final String[] output3 = {
      "IoTDB> error format of max print row count, it should be an integer number"
    };
    ProcessBuilder builder3 =
        new ProcessBuilder(
            "sh",
            dir + File.separator + "sbin" + File.separator + "start-cli.sh",
            "-maxPRC",
            "-1111111111111111111111111111");
    testOutput(builder3, output3);
  }
}
