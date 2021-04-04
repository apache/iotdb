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
package org.apache.iotdb.cross.tests.tools.importCsv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public abstract class AbstractScript {

  protected void testOutput(ProcessBuilder builder, String[] output) throws IOException {
    builder.redirectErrorStream(true);
    Process p = builder.start();
    BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    List<String> actualOutput = new ArrayList<>();
    while (true) {
      line = r.readLine();
      if (line == null) {
        break;
      } else {
        actualOutput.add(line);
      }
    }
    r.close();
    p.destroy();

    System.out.println("should contains:");
    for (String s : output) {
      System.out.println(s);
    }

    System.out.println("actualOutput:");
    for (String out : actualOutput) {
      System.out.println(out);
    }

    assertTrue(actualOutput.get(actualOutput.size() - 1).contains(output[output.length - 1]));
  }

  protected String getCliPath() {
    // This is usually always set by the JVM

    File userDir = new File(System.getProperty("user.dir"));
    if (!userDir.exists()) {
      throw new RuntimeException("user.dir " + userDir.getAbsolutePath() + " doesn't exist.");
    }
    File target =
        new File(
            userDir.getParent() + File.separator + "cli",
            "target"
                + File.separator
                + "maven"
                + "-archiver"
                + File.separator
                + "pom"
                + ".properties");
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(target));
    } catch (IOException e) {
      return "target" + File.separator + "iotdb-cli-";
    }
    return new File(
            userDir.getParent() + File.separator + "cli",
            String.format(
                "target" + File.separator + "%s-%s",
                properties.getProperty("artifactId"),
                properties.getProperty("version")))
        .getAbsolutePath();
  }

  protected abstract void testOnWindows(String[] output) throws IOException;

  protected abstract void testOnUnix(String[] output) throws IOException;
}
