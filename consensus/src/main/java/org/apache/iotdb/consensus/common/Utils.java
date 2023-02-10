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
package org.apache.iotdb.consensus.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static List<Path> listAllRegularFilesRecursively(File rootDir) {
    List<Path> allFiles = new ArrayList<>();
    try {
      Files.walkFileTree(
          rootDir.toPath(),
          new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              if (attrs.isRegularFile()) {
                allFiles.add(file);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
              logger.info("visit file {} failed due to {}", file.toAbsolutePath(), exc);
              return FileVisitResult.TERMINATE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException ioException) {
      logger.error("IOException occurred during listing snapshot directory: ", ioException);
      return Collections.emptyList();
    }
    return allFiles;
  }

  public static class MemorizedFileSizeCalc {
    private List<Path> memorized;
    private final File rootDir;
    private long totalSize;

    public MemorizedFileSizeCalc(File rootDir) {
      this.memorized = Collections.emptyList();
      this.rootDir = rootDir;
      this.totalSize = 0;
    }

    public synchronized long getTotalFolderSize() {
      final List<Path> latest = listAllRegularFilesRecursively(rootDir);

      final List<Path> incremental =
          latest.stream().filter(p -> !memorized.contains(p)).collect(Collectors.toList());

      final List<Path> decremental =
          memorized.stream().filter(p -> !latest.contains(p)).collect(Collectors.toList());

      totalSize += incremental.stream().mapToLong(p -> p.toFile().length()).sum();
      if (decremental.size() == memorized.size()) {
        totalSize = 0;
      } else {
        totalSize -= decremental.stream().mapToLong(p -> p.toFile().length()).sum();
      }

      memorized = latest;
      return totalSize;
    }
  }
}
