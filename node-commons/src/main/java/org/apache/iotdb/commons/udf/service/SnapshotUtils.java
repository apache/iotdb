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

package org.apache.iotdb.commons.udf.service;

import org.apache.iotdb.commons.file.SystemFileFactory;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class SnapshotUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFExecutableManager.class);

  public static boolean takeSnapshotForDir(String source, String snapshotDestination)
      throws IOException {
    final SystemFileFactory systemFileFactory = SystemFileFactory.INSTANCE;
    final File sourceFile = systemFileFactory.getFile(source);
    final File destinationFile = systemFileFactory.getFile(snapshotDestination);
    final File temporaryFile =
        systemFileFactory.getFile(destinationFile.getAbsolutePath() + "-" + UUID.randomUUID());

    if (!sourceFile.exists()) {
      return true;
    }

    FileUtils.deleteQuietly(temporaryFile);
    FileUtils.forceMkdir(temporaryFile);

    try {
      FileUtils.copyDirectory(sourceFile, temporaryFile);
      FileUtils.deleteQuietly(destinationFile);
      return temporaryFile.renameTo(destinationFile);
    } finally {
      FileUtils.deleteQuietly(temporaryFile);
    }
  }

  public static void loadSnapshotForDir(String snapshotSource, String destination)
      throws IOException {
    final SystemFileFactory systemFileFactory = SystemFileFactory.INSTANCE;
    final File sourceFile = systemFileFactory.getFile(snapshotSource);
    final File destinationFile = systemFileFactory.getFile(destination);
    final File temporaryFile =
        systemFileFactory.getFile(destinationFile.getAbsolutePath() + "-" + UUID.randomUUID());

    if (!sourceFile.exists()) {
      return;
    }

    try {
      if (destinationFile.exists()) {
        FileUtils.deleteQuietly(temporaryFile);
        FileUtils.moveDirectory(destinationFile, temporaryFile);
      }

      FileUtils.forceMkdir(destinationFile);

      try {
        FileUtils.copyDirectory(sourceFile, destinationFile);
      } catch (Exception e) {
        LOGGER.error("Failed to load udf snapshot and rollback.");
        FileUtils.deleteQuietly(destinationFile);

        if (temporaryFile.exists()) {
          FileUtils.moveDirectory(temporaryFile, destinationFile);
        }
      }
    } finally {
      FileUtils.deleteQuietly(temporaryFile);
    }
  }

  private SnapshotUtils() {}
}
