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
package org.apache.iotdb.db.script;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** This test case is strange that it relies on the maven output files */
public class EnvScriptIT {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void test() throws IOException {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testStartClientOnWindows(".bat", os);
    } else {
      testStartClientOnUnix(".sh", os);
    }
  }

  private void testStartClientOnWindows(String suffix, String os) throws IOException {
    String dir = getServerPath();
    final String output =
        "If you want to change this configuration, please check conf\\datanode-env.bat.";
    String cmd = dir + File.separator + "conf" + File.separator + "datanode-env" + suffix;
    ProcessBuilder startBuilder = new ProcessBuilder("cmd.exe", "/c", cmd);
    testOutput(dir, suffix, startBuilder, output, os);
  }

  private void testStartClientOnUnix(String suffix, String os) throws IOException {
    String dir = getServerPath();
    final String output =
        "If you want to change this configuration, please check conf/datanode-env.sh.";
    String cmd = dir + File.separator + "conf" + File.separator + "datanode-env" + suffix;
    ProcessBuilder builder = new ProcessBuilder("bash", cmd);
    testOutput(cmd, suffix, builder, output, os);
  }

  private void testOutput(
      String cmd, String suffix, ProcessBuilder builder, String output, String os)
      throws IOException {
    builder.redirectErrorStream(true);
    Process startProcess = builder.start();
    BufferedReader startReader =
        new BufferedReader(new InputStreamReader(startProcess.getInputStream()));
    List<String> runtimeOuput = new ArrayList<>();
    String line;
    try {
      while (true) {
        line = startReader.readLine();
        if (line == null) {
          break;
        }
        runtimeOuput.add(line);
      }
      assertEquals(output, runtimeOuput.get(runtimeOuput.size() - 1));
    } finally {
      startReader.close();
      startProcess.destroy();
      runtimeOuput.clear();
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

  protected String getServerPath() {
    // This is usually always set by the JVM
    File userDir = new File(System.getProperty("user.dir"));
    if (!userDir.exists()) {
      throw new RuntimeException("user.dir " + userDir.getAbsolutePath() + " doesn't exist.");
    }
    File target = new File(userDir, "target/maven-archiver/pom.properties");
    Properties properties = new Properties();
    assertTrue(target.exists());
    try {
      properties.load(new FileReader(target));
    } catch (IOException e) {
      return "target/iotdb-server-";
    }
    return new File(
            userDir,
            String.format(
                "target/%s-%s",
                properties.getProperty("artifactId"), properties.getProperty("version")))
        .getAbsolutePath();
  }
}
