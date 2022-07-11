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

package org.apache.iotdb.consensus.multileader.logdispatcher;

import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** An index controller class to balance the performance degradation of frequent disk I/O. */
@ThreadSafe
public class IndexController {

  private final Logger logger = LoggerFactory.getLogger(IndexController.class);

  public static final int FLUSH_INTERVAL = 500;

  private long lastFlushedIndex;
  private long currentIndex;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final String storageDir;
  private final String prefix;

  public IndexController(String storageDir, String prefix) {
    this.storageDir = storageDir;
    this.prefix = prefix + '-';
    restore();
  }

  public long incrementAndGet() {
    try {
      lock.writeLock().lock();
      currentIndex++;
      checkPersist();
      return currentIndex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public long updateAndGet(long index) {
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
      checkPersist();
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

  private void checkPersist() {
    if (currentIndex - lastFlushedIndex >= FLUSH_INTERVAL) {
      persist();
    }
  }

  private void persist() {
    long flushIndex = currentIndex - currentIndex % FLUSH_INTERVAL;
    File oldFile = new File(storageDir, prefix + lastFlushedIndex);
    File newFile = new File(storageDir, prefix + flushIndex);
    try {
      if (oldFile.exists()) {
        FileUtils.moveFile(oldFile, newFile);
      }
      logger.info(
          "Version file updated, previous: {}, current: {}",
          oldFile.getAbsolutePath(),
          newFile.getAbsolutePath());
      lastFlushedIndex = flushIndex;
    } catch (IOException e) {
      logger.error("Error occurred when flushing next version", e);
    }
  }

  private void restore() {
    File directory = new File(storageDir);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(prefix));
    File versionFile;
    if (versionFiles != null && versionFiles.length > 0) {
      long maxVersion = 0;
      int maxVersionIndex = 0;
      for (int i = 0; i < versionFiles.length; i++) {
        long fileVersion = Long.parseLong(versionFiles[i].getName().split("-")[1]);
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
      versionFile = new File(directory, prefix + "0");
      try {
        Files.createFile(versionFile.toPath());
      } catch (IOException e) {
        logger.error("Error occurred when creating new file {}", versionFile.getAbsolutePath(), e);
      }
    }
  }
}
