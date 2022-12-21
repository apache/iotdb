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

import org.apache.iotdb.it.framework.IoTDBTestRunner;

import org.apache.thrift.annotation.Nullable;
import org.junit.runner.RunWith;

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
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
public abstract class AbstractScript {

  protected void testOutput(ProcessBuilder builder, @Nullable String[] output, int statusCode)
      throws IOException {
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

    System.out.println("Process output:");
    for (String s : outputList) {
      System.out.println(s);
    }

    if (output != null) {
      for (int i = 0; i < output.length; i++) {
        assertTrue(
            outputList.get(outputList.size() - 1 - i).contains(output[output.length - 1 - i]));
      }
    }
    while (p.isAlive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail();
      }
    }
    assertEquals(statusCode, p.exitValue());
  }

  protected String getCliPath() {
    // This is usually always set by the JVM

    File userDir = new File(System.getProperty("user.dir"));
    if (!userDir.exists()) {
      throw new RuntimeException("user.dir " + userDir.getAbsolutePath() + " doesn't exist.");
    }
    File target = new File(userDir, "target/maven-archiver/pom.properties");
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(target));
    } catch (IOException e) {
      return "target/iotdb-cli-";
    }
    return new File(
            userDir,
            String.format(
                "target/%s-%s",
                properties.getProperty("artifactId"), properties.getProperty("version")))
        .getAbsolutePath();
  }

  protected abstract void testOnWindows() throws IOException;

  protected abstract void testOnUnix() throws IOException;
}
