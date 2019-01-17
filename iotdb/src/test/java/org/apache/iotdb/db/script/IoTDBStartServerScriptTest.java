/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.script;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;

public class IoTDBStartServerScriptTest {

  private final String START_IOTDB_STR = "IoTDB has started.";

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  // Skip this test for now because if you close IoTDB by stop-server script, it cannot detect whether it is closed or
  // not.
  // @Test
  public void test() throws IOException, InterruptedException {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testStartClientOnWindows(".bat", os);
    } else {
      testStartClientOnUnix(".sh", os);
    }
  }

  private void testStartClientOnWindows(String suffix, String os) throws IOException {
    final String[] output = {"````````````````````````", "Starting IoTDB",
        "````````````````````````"};
    String dir = getCurrentPath("cmd.exe", "/c", "echo %cd%");
    String startCMD =
        dir + File.separator + "iotdb" + File.separator + "bin" + File.separator + "start-server"
            + suffix;
    ProcessBuilder startBuilder = new ProcessBuilder("cmd.exe", "/c", startCMD);
    String stopCMD =
        dir + File.separator + "iotdb" + File.separator + "bin" + File.separator + "stop-server"
            + suffix;
    ProcessBuilder stopBuilder = new ProcessBuilder("cmd.exe", "/c", stopCMD);
    testOutput(dir, suffix, startBuilder, stopBuilder, output, os);
  }

  private void testStartClientOnUnix(String suffix, String os) throws IOException {
    String dir = getCurrentPath("pwd");
    final String[] output = {"---------------------", "Starting IoTDB", "---------------------"};
    String startCMD =
        dir + File.separator + "iotdb" + File.separator + "bin" + File.separator + "start-server"
            + suffix;
    ProcessBuilder startBuilder = new ProcessBuilder("sh", startCMD);
    String stopCMD =
        dir + File.separator + "iotdb" + File.separator + "bin" + File.separator + "stop-server"
            + suffix;
    ProcessBuilder stopBuilder = new ProcessBuilder("sh", stopCMD);
    testOutput(dir, suffix, startBuilder, stopBuilder, output, os);
  }

  private void testOutput(String dir, String suffix, ProcessBuilder startBuilder,
      ProcessBuilder stopBuilder,
      String[] output, String os) throws IOException {
    startBuilder.redirectErrorStream(true);
    Process startProcess = startBuilder.start();
    BufferedReader startReader = new BufferedReader(
        new InputStreamReader(startProcess.getInputStream()));
    List<String> runtimeOuput = new ArrayList<>();
    String line;
    try {
      while (true) {
        line = startReader.readLine();
        if (line == null) {
          break;
        }
        runtimeOuput.add(line);
        if (line.indexOf(START_IOTDB_STR) > 0) {
          break;
        }
      }
      for (int i = 0; i < output.length; i++) {
        assertEquals(output[i], runtimeOuput.get(i));
      }
    } finally {
      startReader.close();
      startProcess.destroy();
      if (os.startsWith("windows")) {
        stopBuilder.redirectErrorStream(true);
        Process stopProcess = stopBuilder.start();
        BufferedReader stopReader = new BufferedReader(
            new InputStreamReader(stopProcess.getInputStream()));
        while (true) {
          line = stopReader.readLine();
          if (line == null) {
            break;
          }
          System.out.println(line);
        }
        stopReader.close();
        stopProcess.destroy();
      }
    }
  }

  private String getCurrentPath(String... command) throws IOException {
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);
    Process p = builder.start();
    BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String path = r.readLine();
    return path;
  }
}
