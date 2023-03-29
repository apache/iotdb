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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.ratis.Utils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** An index controller class to balance the performance degradation of frequent disk I/O. */
@ThreadSafe
public class IndexController {

  public static final String SEPARATOR = "-";

  private final Logger logger = LoggerFactory.getLogger(IndexController.class);

  private long lastFlushedIndex;
  private long currentIndex;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final String storageDir;

  private final Peer peer;
  private final String prefix;
  private final long initialIndex;

  private final long checkpointGap;

  public IndexController(String storageDir, Peer peer, long initialIndex, long checkpointGap) {
    this.storageDir = storageDir;
    this.peer = peer;
    this.prefix = peer.getNodeId() + SEPARATOR;
    this.checkpointGap = checkpointGap;
    this.initialIndex = initialIndex;
    // This is because we changed the name of the version file in version 1.0.1. In order to ensure
    // compatibility with version 1.0.0, we need to add this function. We will remove this function
    // in the future version 2.x.
    upgrade();
    restore();
  }

  public long updateAndGet(long index, boolean forcePersist) {
    try {
      lock.writeLock().lock();
      long newCurrentIndex = Math.max(currentIndex, index);
      logger.debug(
          "update index from currentIndex {} to {} for file prefix {} in {}",
          currentIndex,
          newCurrentIndex,
          prefix,
          storageDir);
      currentIndex = newCurrentIndex;
      checkPersist(forcePersist);
      return currentIndex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public long getCurrentIndex() {
    try {
      lock.readLock().lock();
      return currentIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  @TestOnly
  public long getLastFlushedIndex() {
    return lastFlushedIndex;
  }

  private void checkPersist(boolean forcePersist) {
    if (forcePersist || currentIndex - lastFlushedIndex >= checkpointGap) {
      persist();
    }
  }

  private void persist() {
    long flushIndex = currentIndex;
    if (flushIndex == lastFlushedIndex) {
      return;
    }
    File oldFile = new File(storageDir, prefix + lastFlushedIndex);
    File newFile = new File(storageDir, prefix + flushIndex);
    try {
      if (oldFile.exists()) {
        FileUtils.moveFile(oldFile, newFile);
        logger.info(
            "version file updated, previous: {}, current: {}",
            oldFile.getAbsolutePath(),
            newFile.getAbsolutePath());
      } else {
        // In the normal state, this branch should not be triggered.
        // During the DataNode removing stage, the version file towards removing peer may be deleted
        // before all the async operations returns. We needn't add some sync operation here
        // because it won't infect the correctness
        logger.info(
            "failed to flush sync index because previous version file {} does not exists. "
                + "It may be caused by the target Peer is removed from current group. "
                + "target file is {}",
            oldFile.getAbsolutePath(),
            newFile.getAbsolutePath());
      }

      lastFlushedIndex = flushIndex;
    } catch (IOException e) {
      logger.error("Error occurred when flushing next version", e);
    }
  }

  private void upgrade() {
    File directory = new File(storageDir);
    String oldPrefix = Utils.fromTEndPointToString(peer.getEndpoint()) + SEPARATOR;
    Optional.ofNullable(directory.listFiles((dir, name) -> name.startsWith(oldPrefix)))
        .ifPresent(
            files ->
                Arrays.stream(files)
                    .forEach(
                        oldFile -> {
                          String[] splits = oldFile.getName().split(SEPARATOR);
                          long fileVersion = Long.parseLong(splits[splits.length - 1]);
                          File newFile = new File(storageDir, prefix + fileVersion);
                          try {
                            logger.info(
                                "version file upgrade, previous: {}, current: {}",
                                oldFile.getAbsolutePath(),
                                newFile.getAbsolutePath());
                            FileUtils.moveFile(oldFile, newFile);
                          } catch (IOException e) {
                            logger.error("Error occurred when upgrading version file", e);
                          }
                        }));
  }

  private void restore() {
    File directory = new File(storageDir);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(prefix));
    File versionFile;
    if (versionFiles != null && versionFiles.length > 0) {
      long maxVersion = 0;
      int maxVersionIndex = 0;
      for (int i = 0; i < versionFiles.length; i++) {
        long fileVersion = Long.parseLong(versionFiles[i].getName().split(SEPARATOR)[1]);
        if (fileVersion > maxVersion) {
          maxVersion = fileVersion;
          maxVersionIndex = i;
        }
      }
      lastFlushedIndex = maxVersion;
      for (int i = 0; i < versionFiles.length; i++) {
        if (i != maxVersionIndex) {
          try {
            Files.delete(versionFiles[i].toPath());
          } catch (IOException e) {
            logger.error(
                "Delete outdated version file {} failed", versionFiles[i].getAbsolutePath(), e);
          }
        }
      }
      currentIndex = lastFlushedIndex;
    } else {
      currentIndex = initialIndex;
      versionFile = new File(directory, prefix + initialIndex);
      try {
        Files.createFile(versionFile.toPath());
        lastFlushedIndex = initialIndex;
      } catch (IOException e) {
        // TODO: (xingtanzjr) we need to handle the situation that file creation failed.
        //  Or the dispatcher won't run correctly
        logger.error("Error occurred when creating new file {}", versionFile.getAbsolutePath(), e);
      }
    }
  }

  public void cleanupVersionFiles() throws IOException {
    File directory = new File(storageDir);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(prefix));
    if (versionFiles != null && versionFiles.length > 0) {
      for (File versionFile : versionFiles) {
        Files.delete(versionFile.toPath());
      }
    }
  }
}
