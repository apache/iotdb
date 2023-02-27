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

package org.apache.iotdb.consensus.natraft.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("java:S1135")
public class IOUtils {

  private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);

  private IOUtils() {
    // util class
  }

  public static Throwable getRootCause(Throwable e) {
    Throwable curr = e;
    while (curr.getCause() != null) {
      curr = curr.getCause();
    }
    return curr;
  }

  public static List<String> collectRelativePaths(File directory) {
    if (!directory.exists() || !directory.isDirectory()) {
      logger.warn("{} is not a directory", directory);
      return Collections.emptyList();
    }

    String currentRelativePath = "";
    List<String> result = new ArrayList<>();
    collectRelativePaths(directory, currentRelativePath, result);
    return result;
  }

  public static void collectRelativePaths(
      File directory, String currentRelativePath, List<String> result) {
    File[] files = directory.listFiles();
    if (files == null) {
      logger.warn("Cannot list files under {}", directory);
      return;
    }
    for (File file : files) {
      if (file.isDirectory()) {
        collectRelativePaths(file, currentRelativePath + File.separator + file.getName(), result);
      } else {
        result.add(currentRelativePath + File.separator + file.getName());
      }
    }
  }

  public static List<Path> collectPaths(File directory) {
    if (!directory.exists() || !directory.isDirectory()) {
      logger.warn("{} is not a directory", directory);
      return Collections.emptyList();
    }

    List<Path> result = new ArrayList<>();
    collectPaths(directory, result);
    return result;
  }

  public static void collectPaths(File directory, List<Path> result) {
    File[] files = directory.listFiles();
    if (files == null) {
      logger.warn("Cannot list files under {}", directory);
      return;
    }
    for (File file : files) {
      if (file.isDirectory()) {
        collectPaths(file, result);
      } else {
        result.add(file.toPath());
      }
    }
  }

  /**
   * An interface that is used for a node to pull chunks of files like TsFiles. The file should be a
   * temporary hard link, and once the file is totally read, it will be removed.
   */
  public static ByteBuffer readFile(String filePath, long offset, int length) throws IOException {
    File file = new File(filePath);
    if (!file.exists()) {
      logger.warn("Reading a non-existing snapshot file {}", filePath);
      return ByteBuffer.allocate(0);
    }

    ByteBuffer result;
    int len;
    try (BufferedInputStream bufferedInputStream =
        new BufferedInputStream(Files.newInputStream(file.toPath()))) {
      skipExactly(bufferedInputStream, offset);
      byte[] bytes = new byte[length];
      result = ByteBuffer.wrap(bytes);
      len = bufferedInputStream.read(bytes);
      result.limit(Math.max(len, 0));
    }
    return result;
  }

  private static void skipExactly(InputStream stream, long byteToSkip) throws IOException {
    while (byteToSkip > 0) {
      byteToSkip -= stream.skip(byteToSkip);
    }
  }
}
