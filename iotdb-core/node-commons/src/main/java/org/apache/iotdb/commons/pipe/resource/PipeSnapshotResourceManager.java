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

package org.apache.iotdb.commons.pipe.resource;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class PipeSnapshotResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSnapshotResourceManager.class);

  private static final String PIPE_SNAPSHOT_DIR_NAME = "pipe_snapshot";

  private final Set<String> pipeCopiedSnapshotDirs;

  private final Map<String, AtomicLong> copiedSnapshotRoot2ReferenceCountMap = new HashMap<>();

  // locks copiedSnapshotRoot2PinCountMap
  private final ReentrantLock lock = new ReentrantLock();

  protected PipeSnapshotResourceManager(Set<String> pipeCopiedSnapshotDirs) {
    this.pipeCopiedSnapshotDirs = pipeCopiedSnapshotDirs;
  }

  /**
   * Given a snapshot path, copy it to pipe dir, maintain a reference count for the copied snapshot,
   * and return the copied snapshot root path.
   *
   * <p>if the given file is already a copied snapshot, increase its reference count and return it.
   *
   * <p>otherwise, copy the snapshot to pipe dir, increase its reference count and return it.
   *
   * @throws IOException when copy file failed
   */
  public String increaseSnapshotReference(String snapshotRoot) throws IOException {
    lock.lock();
    try {
      // If the snapshot root is already a copied snapshot, just increase and return it
      if (increaseReferenceIfExists(snapshotRoot)) {
        return snapshotRoot;
      }

      // Else, check if there is a related copy in pipe dir. if so, increase and return it
      final String copiedFilePath = getCopiedSnapshotRootInPipeDir(snapshotRoot);
      if (increaseReferenceIfExists(copiedFilePath)) {
        return copiedFilePath;
      }

      // Otherwise, copy the snapshot to pipe dir
      if (!FileUtils.copyDir(new File(snapshotRoot), new File(copiedFilePath))) {
        throw new IOException(
            "Failed to copy snapshot from " + snapshotRoot + " to " + copiedFilePath);
      }
      copiedSnapshotRoot2ReferenceCountMap.put(copiedFilePath, new AtomicLong(1));
      return copiedFilePath;
    } finally {
      lock.unlock();
    }
  }

  private boolean increaseReferenceIfExists(String copiedSnapshotRoot) {
    final AtomicLong referenceCount = copiedSnapshotRoot2ReferenceCountMap.get(copiedSnapshotRoot);
    if (referenceCount != null) {
      referenceCount.incrementAndGet();
      return true;
    }
    return false;
  }

  /**
   * Find the target path of snapshot in pipe dir, by recursively getting parent dir until target
   * dir is found. On ConfigNode, the copied snapshot will be in "consensus/pipe_snapshot", while on
   * DataNode they will be in "consensus/pipe_snapshot", "data/pipe_snapshot",
   * "system/pipe_snapshot".
   *
   * @param snapshotRoot Original snapshot path
   * @return Target snapshot path in pipe dir
   */
  private String getCopiedSnapshotRootInPipeDir(String snapshotRoot) throws IOException {
    final File snapshotRootFile = new File(snapshotRoot);
    File parentFile = snapshotRootFile;
    // recursively find the parent path until meets the pipe snapshot dir
    while (!pipeCopiedSnapshotDirs.contains(parentFile.getName())) {
      if (parentFile.getParentFile() == null) {
        LOGGER.warn("Cannot find correct target snapshot path in pipe dir for {}", snapshotRoot);
        throw new IOException(
            "Cannot find correct target snapshot path in pipe dir for " + snapshotRoot);
      }
      parentFile = parentFile.getParentFile();
    }
    return parentFile.getPath()
        + File.separator
        + PIPE_SNAPSHOT_DIR_NAME
        + File.separator
        + snapshotRootFile.getName();
  }

  /**
   * Decrease the reference count of the snapshot. If the reference count is decreased to 0, delete
   * the snapshot.
   *
   * @param snapshotRoot copied snapshot path to be decreased
   */
  public void decreaseSnapshotReference(String snapshotRoot) {
    lock.lock();
    try {
      final AtomicLong referenceCount = copiedSnapshotRoot2ReferenceCountMap.get(snapshotRoot);
      if (referenceCount == null) {
        return;
      }

      final long count = referenceCount.decrementAndGet();
      if (count == 0) {
        copiedSnapshotRoot2ReferenceCountMap.remove(snapshotRoot);
        FileUtils.deleteDirectory(new File(snapshotRoot));
      }
    } finally {
      lock.unlock();
    }
  }

  @TestOnly
  public long getSnapshotReferenceCount(String snapshotRoot) {
    lock.lock();
    try {
      final AtomicLong count = copiedSnapshotRoot2ReferenceCountMap.get(snapshotRoot);
      return count != null ? count.get() : 0;
    } finally {
      lock.unlock();
    }
  }
}
