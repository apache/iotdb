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

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StartClientScriptIT extends AbstractScript {

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
      testOnWindows();
    } else {
      testOnUnix();
    }
  }

  @Override
  protected void testOnWindows() throws IOException {
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

  @Override
  protected void testOnUnix() throws IOException {
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
}
