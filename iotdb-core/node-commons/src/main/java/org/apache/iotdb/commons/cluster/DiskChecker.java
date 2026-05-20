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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Shared utility used by both ConfigNode and DataNode to evaluate the health of a set of critical
 * directories and, optionally, drive the global {@link NodeStatus} on {@link CommonConfig}.
 *
 * <p>Only transitions between {@link NodeStatus#Running} and {@link NodeStatus#ReadOnly} with
 * reason {@link NodeStatus#DISK_FULL}/{@link NodeStatus#DISK_CRASH} are managed here. Other
 * ReadOnly reasons (e.g. manual) are left untouched.
 */
public class DiskChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiskChecker.class);

  private static final byte[] PROBE_PAYLOAD = new byte[] {0x01};
  private static final String PROBE_PREFIX = "iotdb-disk-probe-";
  private static final String PROBE_SUFFIX = ".tmp";

  public enum DiskStatus {
    NORMAL,
    DISK_FULL,
    DISK_CRASH
  }

  private DiskChecker() {}

  /**
   * Evaluate the supplied directories. A single unwritable directory yields {@link
   * DiskStatus#DISK_CRASH}; any directory whose usable/total ratio is below the threshold yields
   * {@link DiskStatus#DISK_FULL} (when no crash is detected); otherwise {@link DiskStatus#NORMAL}.
   */
  public static DiskStatus check(List<String> dirs, double freeRatioThreshold) {
    if (dirs == null || dirs.isEmpty()) {
      return DiskStatus.NORMAL;
    }
    boolean anyFull = false;
    for (String dir : dirs) {
      if (dir == null || dir.isEmpty()) {
        continue;
      }
      File f = new File(dir);
      if (!f.isDirectory()) {
        LOGGER.warn(CommonMessages.DISK_CRASH_PROBE_FAILED, dir);
        return DiskStatus.DISK_CRASH;
      }
      try {
        Path probe = Files.createTempFile(Paths.get(dir), PROBE_PREFIX, PROBE_SUFFIX);
        try {
          Files.write(probe, PROBE_PAYLOAD);
        } finally {
          Files.deleteIfExists(probe);
        }
      } catch (IOException e) {
        LOGGER.warn(CommonMessages.DISK_CRASH_PROBE_FAILED, dir, e);
        return DiskStatus.DISK_CRASH;
      }
      long total = f.getTotalSpace();
      long usable = f.getUsableSpace();
      if (total > 0 && (double) usable / total < freeRatioThreshold) {
        anyFull = true;
      }
    }
    return anyFull ? DiskStatus.DISK_FULL : DiskStatus.NORMAL;
  }

  /**
   * Run {@link #check} and apply the result to {@link CommonConfig}. See class javadoc for
   * transition rules.
   */
  public static void checkAndApply(List<String> dirs, double freeRatioThreshold) {
    apply(check(dirs, freeRatioThreshold));
  }

  /** Visible for tests; package-public callers should prefer {@link #checkAndApply}. */
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
        if (currentlyFull || currentlyCrash) {
          LOGGER.info(CommonMessages.DISK_RECOVERED_SET_RUNNING, currentReason);
          config.setNodeStatus(NodeStatus.Running);
          config.setStatusReason(null);
        }
        break;
    }
  }
}
