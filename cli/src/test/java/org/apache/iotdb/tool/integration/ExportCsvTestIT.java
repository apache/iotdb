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
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ExportCsvTestIT extends AbstractScript {

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
      testOnWindows("select * from root");
    } else {
      testOnUnix("select * from root");
    }
  }

  @Override
  protected void testOnWindows() {}

  @Override
  protected void testOnUnix() {}

  protected void testOnWindows(String queryCommand) throws IOException {

    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            dir + File.separator + "tools" + File.separator + "export-csv.bat",
            "-h",
            "127.0.0.1",
            "-p",
            "6667",
            "-u",
            "root",
            "-pw",
            "root",
            "-td",
            ".\\",
            "-q",
            queryCommand);
    testOutput(builder, null);
  }

  protected void testOnUnix(String queryCommand) throws IOException {

    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "sh",
            dir + File.separator + "tools" + File.separator + "export-csv.sh",
            "-h",
            "127.0.0.1",
            "-p",
            "6667",
            "-u",
            "root",
            "-pw",
            "root",
            "-td",
            "./",
            "-q",
            queryCommand);
    testOutput(builder, null);
  }

  @Test
  public void testAggregationQuery() throws IOException {
    String os = System.getProperty("os.name").toLowerCase();
    String queryCommand = "select count(*) from root";
    if (os.startsWith("windows")) {
      testOnWindows(queryCommand);
    } else {
      testOnUnix(queryCommand);
    }
  }
}
