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
package org.apache.iotdb.tool.integration;

import org.apache.iotdb.cli.AbstractScript;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ImportCsvTestIT extends AbstractScript {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

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
      "````````````````````````````````````````````````",
      "Starting IoTDB Client Import Script",
      "````````````````````````````````````````````````",
      "Encounter an error when connecting to server, because Fail to reconnect to server. "
          + "Please check server status.127.0.0.1:6668"
    };
    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            dir + File.separator + "tools" + File.separator + "import-csv.bat",
            "-h",
            "127.0.0.1",
            "-p",
            "6668",
            "-u",
            "root",
            "-pw",
            "root",
            "-f",
            "./",
            "&",
            "exit",
            "%^errorlevel%");
    testOutput(builder, output, 1);
  }

  @Override
  protected void testOnUnix() throws IOException {
    final String[] output = {
      "------------------------------------------",
      "Starting IoTDB Client Import Script",
      "------------------------------------------",
      "Encounter an error when connecting to server, because Fail to reconnect to server. "
          + "Please check server status.127.0.0.1:6668"
    };
    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "sh",
            dir + File.separator + "tools" + File.separator + "import-csv.sh",
            "-h",
            "127.0.0.1",
            "-p",
            "6668",
            "-u",
            "root",
            "-pw",
            "root",
            "-f",
            "./");
    testOutput(builder, output, 1);
  }
}
