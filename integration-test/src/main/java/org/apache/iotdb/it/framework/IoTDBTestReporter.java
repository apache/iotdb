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
package org.apache.iotdb.it.framework;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class IoTDBTestReporter {

  public static void main(String[] args) throws IOException {
    List<IoTDBTestStat> stats = new ArrayList<>();
    Path outputDirPath =
        Paths.get(
            System.getProperty("user.dir"), "integration-test", IoTDBTestListener.statOutputDir);
    File outputDir = outputDirPath.toFile();
    if (!outputDir.exists() || !outputDir.isDirectory()) {
      IoTDBTestLogger.logger.error(
          "the output dir {} is not a valid directory, the reporter will be aborted",
          outputDirPath);
      return;
    }
    try (Stream<Path> s = Files.walk(outputDirPath)) {
      s.forEach(
          source -> {
            if (source.toString().endsWith(IoTDBTestListener.statExt)) {
              try {
                List<String> lines = Files.readAllLines(source);
                for (String l : lines) {
                  String[] parts = l.split("\t");
                  if (parts.length == 2) {
                    IoTDBTestStat stat = new IoTDBTestStat(parts[1], Double.parseDouble(parts[0]));
                    stats.add(stat);
                  }
                }
              } catch (IOException e) {
                IoTDBTestLogger.logger.error("read stats file failed", e);
              }
            }
          });
    }
    Collections.sort(stats);
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("==== Top 30 slowest cases ====\n");
    for (int i = 0; i < Math.min(30, stats.size()); i++) {
      sb.append(stats.get(i)).append("\n");
    }
    IoTDBTestLogger.logger.info(sb.toString());
  }
}
