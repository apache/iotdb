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

package org.apache.iotdb.db.engine.version;

import org.apache.iotdb.commons.file.SystemFileFactory;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/** SimpleFileVersionController uses a local file and its file name to store the version. */
public class SimpleFileVersionController implements VersionController {

  private static final Logger logger = LoggerFactory.getLogger(SimpleFileVersionController.class);
  public static final String FILE_PREFIX = "Version-";
  public static final String UPGRADE_DIR = "upgrade";
  /**
   * Every time currVersion - prevVersion >= saveInterval, currVersion is persisted and prevVersion
   * is set to currVersion. When recovering from file, the version number is automatically increased
   * by saveInterval to avoid conflicts.
   */
  private static long saveInterval = 100;
  /** time partition id to dividing time series into different database */
  private long timePartitionId;

  private long prevVersion;
  private long currVersion;
  private String directoryPath;

  public SimpleFileVersionController(String directoryPath, long timePartitionId)
      throws IOException {
    this.directoryPath = directoryPath + File.separator + timePartitionId;
    this.timePartitionId = timePartitionId;
    restore();
  }

  /** only used for upgrading */
  public SimpleFileVersionController(String directoryPath) throws IOException {
    this.directoryPath = directoryPath + File.separator + UPGRADE_DIR;
    restore();
  }

  public static long getSaveInterval() {
    return saveInterval;
  }

  // test only method
  public static void setSaveInterval(long saveInterval) {
    SimpleFileVersionController.saveInterval = saveInterval;
  }

  public long getTimePartitionId() {
    return timePartitionId;
  }

  public void setTimePartitionId(long timePartitionId) {
    this.timePartitionId = timePartitionId;
  }

  @Override
  public synchronized long nextVersion() {
    currVersion++;
    try {
      checkPersist();
    } catch (IOException e) {
      logger.error("Error occurred when getting next version.", e);
    }
    return currVersion;
  }

  /**
   * Test only method, no need for concurrency.
   *
   * @return the current version.
   */
  @Override
  public long currVersion() {
    return currVersion;
  }

  private void checkPersist() throws IOException {
    if ((currVersion - prevVersion) >= saveInterval) {
      persist();
    }
  }

  private void persist() throws IOException {
    File oldFile = SystemFileFactory.INSTANCE.getFile(directoryPath, FILE_PREFIX + prevVersion);
    File newFile = SystemFileFactory.INSTANCE.getFile(directoryPath, FILE_PREFIX + currVersion);
    if (oldFile.exists()) {
      FileUtils.moveFile(oldFile, newFile);
    }
    logger.info(
        "Version file updated, previous: {}, current: {}",
        oldFile.getAbsolutePath(),
        newFile.getAbsolutePath());
    prevVersion = currVersion;
  }

  /** recovery from disk */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void restore() throws IOException {
    File directory = SystemFileFactory.INSTANCE.getFile(directoryPath);
    if (!directory.exists()) {
      directory.mkdirs();
    }
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(FILE_PREFIX));
    File versionFile;
    if (versionFiles != null && versionFiles.length > 0) {
      long maxVersion = 0;
      int maxVersionIndex = 0;
      for (int i = 0; i < versionFiles.length; i++) {
        // extract version from "Version-123456"
        long fileVersion = Long.parseLong(versionFiles[i].getName().split("-")[1]);
        if (fileVersion > maxVersion) {
          maxVersion = fileVersion;
          maxVersionIndex = i;
        }
      }
      prevVersion = maxVersion;
      for (int i = 0; i < versionFiles.length; i++) {
        if (i != maxVersionIndex) {
          versionFiles[i].delete();
        }
      }
    } else {
      versionFile = SystemFileFactory.INSTANCE.getFile(directory, FILE_PREFIX + "0");
      prevVersion = 0;
      if (!versionFile.createNewFile()) {
        logger.warn("Cannot create new version file {}", versionFile);
      }
    }
    // prevent overlapping in case of failure
    currVersion = prevVersion + saveInterval;
    persist();
  }
}
