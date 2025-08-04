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

package org.apache.iotdb.db.blame;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ListFile {

  private static final Set<String> safeSet =
      new HashSet<>(Arrays.asList("java", "org.apache.iotdb", "org.apache.tsfile"));
  private static final Set<String> bigSet = new HashSet<>();

  private static final Set<String> jarSet =
      new HashSet<>(
          Arrays.asList(
              // "caffeine",
              // "apache.commons",
              // guava
              // "common.util",
              // "thridparty",
              // "zstd",
              // "snappy",
              // lz4
              // "jpountz",
              // "airline",
              // "antlr",
              // "jetty",
              // "apache.thrift",
              // bsd
              "opcua"
              // "disruptor",
              // "HdrHistogram",
              // "LatencyUtils",
              // "codahale.metrics",
              // "micrometer",
              // "reactor",
              // "io.netty",
              // "cglib",
              // "moquette",
              // "slf4j",
              // "logback",
              // "openapi",
              // "pax",
              // "gson",
              // "ratis"
              ));

  public static void main(String[] args) throws IOException {
    for (final String jar : jarSet) {
      System.out.println(jar + ":");
      traverseDirectory(
          jar, new File("C:\\Users\\13361\\Downloads\\timechodb-rc-1.1.3\\timechodb-rc-1.1.3.1"));
      bigSet.stream()
          .sorted()
          .forEach(str -> System.out.println(str.replace(".", "\\").replace("+", ".") + "java"));
      bigSet.clear();
      System.out.println();
    }
  }

  public static void traverseDirectory(final String jar, File directory) throws IOException {
    // 列出目录中的所有文件和子目录
    File[] files = directory.listFiles();

    final boolean contains = directory.getPath().contains("src" + File.separator + "test");
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          // 如果是目录，递归调用
          traverseDirectory(jar, file);
        } else if (!contains && file.getName().endsWith(".java")) {
          try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String str;
            while ((str = reader.readLine()) != null) {
              str = str.trim();
              boolean has = false;
              if (str.startsWith("import ")) {
                str = str.substring(7);
                if (str.startsWith("static")) {
                  str = str.substring(7);
                }
                for (final String safe : safeSet) {
                  if (str.startsWith(safe)) {
                    has = true;
                    break;
                  }
                }
                if (has) {
                  continue;
                }
                str = str.replace(";", "") + "+";
                if (str.contains(jar)) {
                  bigSet.add(str);
                }
              }
            }
          }
        }
      }
    }
  }
}
