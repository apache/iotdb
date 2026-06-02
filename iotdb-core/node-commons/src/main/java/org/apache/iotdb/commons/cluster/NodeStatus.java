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

import org.apache.iotdb.commons.i18n.CommonMessages;

import java.util.OptionalInt;

/** Node status for showing cluster */
public enum NodeStatus {
  /** Node running properly */
  Running("Running"),

  /** Node connection failure */
  Unknown("Unknown"),

  /** Node is in removing */
  Removing("Removing"),

  /** Only query statements are permitted */
  ReadOnly("ReadOnly");
  public static final String DISK_FULL = "DiskFull";
  public static final String DISK_CRASH = "DiskCrash";

  private final String status;

  NodeStatus(String status) {
    this.status = status;
  }

  public String getStatus() {
    return status;
  }

  public static NodeStatus parse(String status) {
    for (NodeStatus nodeStatus : NodeStatus.values()) {
      if (nodeStatus.status.equals(status)) {
        return nodeStatus;
      }
    }
    throw new RuntimeException(String.format(CommonMessages.NODE_STATUS_NOT_EXIST, status));
  }

  public static boolean isNormalStatus(NodeStatus status) {
    // Currently, the only normal status is Running
    return status != null && status.equals(NodeStatus.Running);
  }

  /**
   * Map a node's {@link NodeStatus} (and optional reason) to the Ratis peer priority that should
   * govern its candidacy in leader elections.
   *
   * <pre>
   *   Running                          →   0   (full candidate)
   *   ReadOnly + {@link #DISK_FULL}    →  -1   (out-rank healthy peers but ahead of crashed)
   *   ReadOnly + {@link #DISK_CRASH}   →  -2   (most degraded — last choice)
   *   anything else                    →  empty (priority must not be changed)
   * </pre>
   *
   * Returning {@link OptionalInt#empty()} for Unknown/Removing/manual ReadOnly keeps transient
   * blips and operator-driven states from rewriting peer priorities.
   */
  public static OptionalInt priorityForStatus(NodeStatus status, String statusReason) {
    if (Running.equals(status)) {
      return OptionalInt.of(0);
    }
    if (ReadOnly.equals(status)) {
      if (DISK_CRASH.equals(statusReason)) {
        return OptionalInt.of(-2);
      }
      if (DISK_FULL.equals(statusReason)) {
        return OptionalInt.of(-1);
      }
    }
    return OptionalInt.empty();
  }

  public static boolean isReadable(NodeStatus status) {
    switch (status) {
      case Running:
      case Removing:
      case ReadOnly:
        return true;
      case Unknown:
        return false;
      default:
        throw new UnsupportedOperationException(
            String.format(CommonMessages.UNKNOWN_NODE_STATUS, status));
    }
  }
}
