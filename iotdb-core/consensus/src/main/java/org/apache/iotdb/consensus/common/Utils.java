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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;

import org.apache.commons.io.FileUtils;
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
import java.util.regex.Pattern;

public class Utils {
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static String buildPeerDir(File storageDir, ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
  }

  private Utils() {}

  public static List<Path> listAllRegularFilesRecursively(File rootDir) {
    List<Path> allFiles = new ArrayList<>();
    try {
      Files.walkFileTree(
          rootDir.toPath(),
          new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              if (attrs.isRegularFile()) {
                allFiles.add(file);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
              logger.info("visit file {} failed due to {}", file.toAbsolutePath(), exc);
              return FileVisitResult.TERMINATE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException ioException) {
      logger.error("IOException occurred during listing snapshot directory: ", ioException);
      return Collections.emptyList();
    }
    return allFiles;
  }

  public static long getLatestSnapshotIndex(
      String storageDir, String snapshotDirName, Pattern snapshotIndexPattern) {
    long snapShotIndex = 0;
    File directory = new File(storageDir);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(snapshotDirName));
    if (versionFiles == null || versionFiles.length == 0) {
      return snapShotIndex;
    }
    for (File file : versionFiles) {
      snapShotIndex =
          Math.max(
              snapShotIndex,
              Long.parseLong(snapshotIndexPattern.matcher(file.getName()).replaceAll("")));
    }
    return snapShotIndex;
  }

  public static void clearOldSnapshot(
      String storageDir, String snapshotDirName, String newSnapshotDirName) {
    File directory = new File(storageDir);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(snapshotDirName));
    if (versionFiles == null || versionFiles.length == 0) {
      logger.error("Can not find any snapshot dir after build a new snapshot");
      return;
    }
    for (File file : versionFiles) {
      if (!file.getName().equals(newSnapshotDirName)) {
        try {
          FileUtils.deleteDirectory(file);
        } catch (IOException e) {
          logger.error("Delete old snapshot dir {} failed", file.getAbsolutePath(), e);
        }
      }
    }
  }
}
