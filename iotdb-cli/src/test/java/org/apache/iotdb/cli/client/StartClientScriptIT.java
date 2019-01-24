/**
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
package org.apache.iotdb.cli.client;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StartClientScriptIT {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() throws IOException, InterruptedException {

    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testStartClientOnWindows();
    } else {
      testStartClientOnUnix();
    }
  }

  private void testStartClientOnWindows() throws IOException {
    final String[] output = {"````````````````````````", "Starting IoTDB Client",
        "````````````````````````",
        "IoTDB> Connection Error, please check whether the network is available or the server has started. Host is 127.0.0.1, port is 6668."};
    String dir = getCurrentPath("cmd.exe", "/c", "echo %cd%");
    ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c",
        dir + File.separator + "cli" + File.separator + "bin" + File.separator + "start-client.bat",
        "-h",
        "127.0.0.1", "-p", "6668", "-u", "root", "-pw", "root");
    testOutput(builder, output);
  }

  private void testStartClientOnUnix() throws IOException {
    final String[] output = {"---------------------", "Starting IoTDB Client",
        "---------------------",
        "IoTDB> Connection Error, please check whether the network is available or the server has started. Host is 127.0.0.1, port is 6668."};
    String dir = getCurrentPath("pwd");
    System.out.println(dir);
    ProcessBuilder builder = new ProcessBuilder("sh",
        dir + File.separator + "cli" + File.separator + "bin" + File.separator + "start-client.sh",
        "-h",
        "127.0.0.1", "-p", "6668", "-u", "root", "-pw", "root");
    testOutput(builder, output);
  }

  private void testOutput(ProcessBuilder builder, String[] output) throws IOException {
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

    for (int i = 0; i < output.length; i++) {
      assertEquals(output[output.length - 1 - i], outputList.get(outputList.size() - 1 - i));
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
