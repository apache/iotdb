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
package org.apache.iotdb.commons.disk.strategy;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.i18n.UtilMessages;
import org.apache.iotdb.commons.utils.JVMCommonUtils;
import org.apache.iotdb.commons.utils.TestOnly;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Selects the folder with the least occupied space.
 *
 * <p>Computing the occupied space of a folder requires a full {@code Files.walk} of its directory
 * tree, which is very expensive when a folder holds a huge number of (small) files, e.g. while a
 * snapshot consisting of hundreds of thousands of tiny files is being received. Re-walking on every
 * selection turned the per-file cost into a full-tree scan and made the overall cost quadratic.
 *
 * <p>To avoid that, the occupied space of every folder is cached and only recomputed periodically.
 * Between two refreshes an incremental selection counter is maintained; once enough folders have
 * been selected (or enough time has elapsed) the cached state is reset and the occupied space is
 * recomputed. This keeps the selection semantics (pick the least occupied folder) while bounding
 * the number of full directory scans.
 */
public class MinFolderOccupiedSpaceFirstStrategy extends DirectoryStrategy {

  private long refreshIntervalMs =
      CommonDescriptor.getInstance().getConfig().getMinFolderOccupiedSpaceCacheRefreshIntervalMs();
  private int refreshSelectionThreshold =
      CommonDescriptor.getInstance()
          .getConfig()
          .getMinFolderOccupiedSpaceCacheRefreshSelectionThreshold();

  /** Cached occupied space per folder, captured at the last refresh. */
  private long[] cachedOccupiedSpace;

  /** Incremental count of selections made since the last refresh. */
  private int selectionsSinceRefresh;

  /** Timestamp (ms) of the last refresh; a negative value means the cache must be (re)built. */
  private long lastRefreshTimeMs = -1;

  @Override
  public int nextFolderIndex() throws DiskSpaceInsufficientException {
    return getMinOccupiedSpaceFolder();
  }

  private synchronized int getMinOccupiedSpaceFolder() throws DiskSpaceInsufficientException {
    if (needRefresh()) {
      refreshOccupiedSpace();
    }

    int minIndex = -1;
    long minSpace = Long.MAX_VALUE;

    for (int i = 0; i < folders.size(); i++) {
      String folder = folders.get(i);
      if (isUnavailableFolder(folder)) {
        continue;
      }
      if (!JVMCommonUtils.hasSpace(folder)) {
        continue;
      }
      long space = cachedOccupiedSpace[i];
      if (space < minSpace) {
        minSpace = space;
        minIndex = i;
      }
    }

    if (minIndex == -1) {
      throw new DiskSpaceInsufficientException(folders);
    }

    selectionsSinceRefresh++;
    return minIndex;
  }

  private boolean needRefresh() {
    if (cachedOccupiedSpace == null || cachedOccupiedSpace.length != folders.size()) {
      return true;
    }
    if (lastRefreshTimeMs < 0 || selectionsSinceRefresh >= refreshSelectionThreshold) {
      return true;
    }
    return System.currentTimeMillis() - lastRefreshTimeMs >= refreshIntervalMs;
  }

  /** Recompute the occupied space of every folder and reset the incremental state. */
  private void refreshOccupiedSpace() {
    if (cachedOccupiedSpace == null || cachedOccupiedSpace.length != folders.size()) {
      cachedOccupiedSpace = new long[folders.size()];
    }
    for (int i = 0; i < folders.size(); i++) {
      String folder = folders.get(i);
      if (isUnavailableFolder(folder) || !JVMCommonUtils.hasSpace(folder)) {
        // Folder is not a selection candidate; keep it deprioritized without paying for a walk.
        cachedOccupiedSpace[i] = Long.MAX_VALUE;
        continue;
      }
      try {
        cachedOccupiedSpace[i] = JVMCommonUtils.getOccupiedSpace(folder);
      } catch (IOException | UncheckedIOException e) {
        LOGGER.warn(UtilMessages.CANNOT_CALCULATE_OCCUPIED_SPACE, folder, e);
      }
    }
    selectionsSinceRefresh = 0;
    lastRefreshTimeMs = System.currentTimeMillis();
  }

  @TestOnly
  public void setRefreshIntervalMs(long refreshIntervalMs) {
    this.refreshIntervalMs = refreshIntervalMs;
  }

  @TestOnly
  public void setRefreshSelectionThreshold(int refreshSelectionThreshold) {
    this.refreshSelectionThreshold = refreshSelectionThreshold;
  }

  /** Forces the next selection to recompute the occupied space of every folder. */
  @TestOnly
  public synchronized void invalidateCache() {
    this.lastRefreshTimeMs = -1;
  }

  @TestOnly
  public synchronized int getSelectionsSinceRefresh() {
    return selectionsSinceRefresh;
  }
}
