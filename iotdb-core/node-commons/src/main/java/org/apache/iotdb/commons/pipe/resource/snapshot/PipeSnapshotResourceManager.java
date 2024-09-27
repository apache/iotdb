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

package org.apache.iotdb.commons.pipe.resource.snapshot;

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

  public static final String PIPE_SNAPSHOT_DIR_NAME = "pipe_snapshot";

  private final Set<String> pipeCopiedSnapshotDirs;

  private final Map<String, AtomicLong> copiedSnapshotPath2ReferenceCountMap = new HashMap<>();

  // locks copiedSnapshotRoot2PinCountMap
  private final ReentrantLock lock = new ReentrantLock();

  protected PipeSnapshotResourceManager(Set<String> pipeCopiedSnapshotDirs) {
    this.pipeCopiedSnapshotDirs = pipeCopiedSnapshotDirs;
  }

  /**
   * Given a snapshot path, copy it to pipe dir, maintain a reference count for the copied snapshot,
   * and return the copied snapshot root path.
   *
   * <p>If the given file is already a copied snapshot, increase its reference count and return it.
   *
   * <p>Otherwise, copy the snapshot to pipe dir, increase its reference count and return it.
   *
   * @throws IOException when copy file failed
   */
  public String increaseSnapshotReference(String snapshotPath) throws IOException {
    lock.lock();
    try {
      // If the snapshot root is already a copied snapshot, just increase and return it
      if (increaseReferenceIfExists(snapshotPath)) {
        return snapshotPath;
      }

      // Else, check if there is a related copy in pipe dir. if so, increase and return it
      final String copiedFilePath = getCopiedSnapshotPathInPipeDir(snapshotPath);
      if (increaseReferenceIfExists(copiedFilePath)) {
        return copiedFilePath;
      }

      // Otherwise, copy the snapshot to pipe dir
      FileUtils.copyFile(new File(snapshotPath), new File(copiedFilePath));
      copiedSnapshotPath2ReferenceCountMap.put(copiedFilePath, new AtomicLong(1));
      return copiedFilePath;
    } finally {
      lock.unlock();
    }
  }

  private boolean increaseReferenceIfExists(String copiedSnapshotPath) {
    final AtomicLong referenceCount = copiedSnapshotPath2ReferenceCountMap.get(copiedSnapshotPath);
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
   * @param snapshotPath Original snapshot path
   * @return Target snapshot path in pipe dir
   */
  private String getCopiedSnapshotPathInPipeDir(String snapshotPath) throws IOException {
    File parentFile = new File(snapshotPath);
    // Recursively find the parent path until meets the pipe snapshot dir
    while (!pipeCopiedSnapshotDirs.contains(parentFile.getName())) {
      if (parentFile.getParentFile() == null) {
        LOGGER.warn("Cannot find correct target snapshot path in pipe dir for {}", snapshotPath);
        throw new IOException(
            "Cannot find correct target snapshot path in pipe dir for " + snapshotPath);
      }
      parentFile = parentFile.getParentFile();
    }
    // Insert the pipe snapshot dir name into "consensus" and group id to identify the different
    // snapshots. A typical ratis snapshot path may be like
    // data/confignode/consensus/47474747-4747-4747-4747-000000000000/sm/2_58/system/users/root.profile
    // Then the copied pipe snapshot path is like
    // data/confignode/consensus/pipe_snapshot/47474747-4747-4747-4747-000000000000/sm/2_58/system/users/root.profile
    return parentFile.getPath()
        + File.separator
        + PIPE_SNAPSHOT_DIR_NAME
        + snapshotPath.replace(parentFile.getPath(), "");
  }

  /**
   * Decrease the reference count of the snapshot. If the reference count is decreased to 0, delete
   * the snapshot.
   *
   * @param snapshotPath copied snapshot path to be decreased
   */
  public void decreaseSnapshotReference(String snapshotPath) {
    lock.lock();
    try {
      final AtomicLong referenceCount = copiedSnapshotPath2ReferenceCountMap.get(snapshotPath);
      if (referenceCount == null) {
        return;
      }

      final long count = referenceCount.decrementAndGet();
      if (count == 0) {
        copiedSnapshotPath2ReferenceCountMap.remove(snapshotPath);
        FileUtils.deleteFileOrDirectory(new File(snapshotPath));
      }
    } finally {
      lock.unlock();
    }
  }

  @TestOnly
  public long getSnapshotReferenceCount(String snapshotPath) {
    lock.lock();
    try {
      final AtomicLong count = copiedSnapshotPath2ReferenceCountMap.get(snapshotPath);
      return count != null ? count.get() : 0;
    } finally {
      lock.unlock();
    }
  }
}
