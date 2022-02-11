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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.thrift.annotation.Nullable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public abstract class AbstractScript {
  protected String[] command;
  protected final String CSV_FILE = "target" + File.separator + "test.csv";

  protected void testOutput(ProcessBuilder builder, @Nullable String[] output) throws IOException {
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

    if (output != null) {
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

  protected void testMethod(@Nullable String[] params, @Nullable String[] output)
      throws IOException {
    String[] basicParams =
        new String[] {"-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root"};
    command = ArrayUtils.addAll(command, basicParams);
    command = ArrayUtils.addAll(command, params);
    if (params != null) {
      command = ArrayUtils.addAll(command, basicParams);
    }
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    testOutput(processBuilder, output);
  }

  protected static CSVParser readCsvFile(String path) throws IOException {
    return CSVFormat.EXCEL
        .withQuote('\'')
        .withEscape('\\')
        .parse(new InputStreamReader(new FileInputStream(path)));
  }
}
