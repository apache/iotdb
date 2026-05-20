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

package org.apache.iotdb.commons.cluster;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.i18n.CommonMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Shared utility that drives the global {@link NodeStatus} on {@link CommonConfig} based on disk
 * health signals.
 *
 * <p>Two sources feed it:
 *
 * <ul>
 *   <li>{@link #checkFreeRatioAndApply} polls usable / total space across critical directories and
 *       sets {@code ReadOnly(DISK_FULL)} when the ratio drops below a threshold.
 *   <li>{@link #apply}{@code (DISK_CRASH)} is called from passive failure observers — Ratis
 *       write-path catch blocks on ConfigNode and {@code FolderManager.ABNORMAL} aggregation on
 *       DataNode — when a real write IO error has already occurred.
 * </ul>
 *
 * <p>Only transitions between {@link NodeStatus#Running} and {@link NodeStatus#ReadOnly} with
 * reason {@link NodeStatus#DISK_FULL}/{@link NodeStatus#DISK_CRASH} are managed here. Other
 * ReadOnly reasons (e.g. manual) are left untouched.
 */
public class DiskChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiskChecker.class);

  public enum DiskStatus {
    NORMAL,
    DISK_FULL,
    DISK_CRASH
  }

  private DiskChecker() {}

  /**
   * Evaluate the usable/total space ratio of each directory. Any directory whose ratio is below
   * {@code freeRatioThreshold} yields {@link DiskStatus#DISK_FULL}; otherwise {@link
   * DiskStatus#NORMAL}. This method never returns {@link DiskStatus#DISK_CRASH} — crash detection
   * is driven by the Ratis write-path observer on ConfigNode and by {@code FolderManager} on
   * DataNode, both of which call {@link #apply} directly.
   */
  public static DiskStatus checkFreeRatio(List<String> dirs, double freeRatioThreshold) {
    if (dirs == null || dirs.isEmpty()) {
      return DiskStatus.NORMAL;
    }
    for (String dir : dirs) {
      if (dir == null || dir.isEmpty()) {
        continue;
      }
      File f = new File(dir);
      long total = f.getTotalSpace();
      long usable = f.getUsableSpace();
      if (total > 0 && (double) usable / total < freeRatioThreshold) {
        return DiskStatus.DISK_FULL;
      }
    }
    return DiskStatus.NORMAL;
  }

  /** Convenience: run {@link #checkFreeRatio} and apply the result to {@link CommonConfig}. */
  public static void checkFreeRatioAndApply(List<String> dirs, double freeRatioThreshold) {
    apply(checkFreeRatio(dirs, freeRatioThreshold));
  }

  /**
   * Apply a precomputed status to {@link CommonConfig}. Priority is {@code DiskCrash > DiskFull >
   * Normal}; recovery to {@code Running} only fires when the active reason was disk-related.
   */
  public static void apply(DiskStatus result) {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    NodeStatus currentStatus = config.getNodeStatus();
    String currentReason = config.getStatusReason();
    boolean currentlyFull =
        NodeStatus.ReadOnly.equals(currentStatus) && NodeStatus.DISK_FULL.equals(currentReason);
    boolean currentlyCrash =
        NodeStatus.ReadOnly.equals(currentStatus) && NodeStatus.DISK_CRASH.equals(currentReason);

    switch (result) {
      case DISK_CRASH:
        if (!currentlyCrash) {
          LOGGER.warn(CommonMessages.DISK_CRASH_SET_READ_ONLY);
          config.setNodeStatus(NodeStatus.ReadOnly);
          config.setStatusReason(NodeStatus.DISK_CRASH);
        }
        break;
      case DISK_FULL:
        // DiskCrash has higher priority — do not downgrade an existing crash to full.
        if (currentlyCrash) {
          return;
        }
        if (!currentlyFull) {
          LOGGER.warn(CommonMessages.DISK_FULL_SET_READ_ONLY);
          config.setNodeStatus(NodeStatus.ReadOnly);
          config.setStatusReason(NodeStatus.DISK_FULL);
        }
        break;
      case NORMAL:
      default:
        // DiskCrash is sticky — only a restart clears it. The free-ratio probe can recover
        // DiskFull alone because free-space reappearing is the literal inverse of running low.
        if (currentlyFull) {
          LOGGER.info(CommonMessages.DISK_RECOVERED_SET_RUNNING, currentReason);
          config.setNodeStatus(NodeStatus.Running);
          config.setStatusReason(null);
        }
        break;
    }
  }
}
