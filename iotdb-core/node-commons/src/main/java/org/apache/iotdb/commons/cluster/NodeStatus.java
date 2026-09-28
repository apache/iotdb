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

/** Node status for showing cluster */
public enum NodeStatus {
  /** Node running properly */
  Running("Running"),

  /** Node connection failure */
  Unknown("Unknown"),

  /** Node is in removing */
  Removing("Removing"),

  /** Only query statements are permitted */
  ReadOnly("ReadOnly"),

  /** Node was stopped intentionally and reported its shutdown */
  Stopped("Stopped");

  /**
   * Reasons for entering ReadOnly. These strings cross node RPCs and are compared literally (e.g.
   * the DiskFull auto-recovery in sampleDiskLoad), so they must stay locale-independent plain
   * constants instead of i18n messages.
   */
  public static final String DISK_FULL = "DiskFull";

  public static final String MANUAL = "Manual";
  public static final String STOPPING = "Stopping";
  public static final String UNRECOVERABLE_ERROR = "UnrecoverableError";

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

  /**
   * Resolves a requested status. Ordinary updates retain Removing and do not replace an explicit
   * Stopped report with Unknown from a heartbeat failure or timeout. A live heartbeat, such as
   * Running after a restart, can revive Stopped. Explicit management updates with force bypass
   * these rules, including when rolling back a removal.
   */
  public static NodeStatus transition(NodeStatus previous, NodeStatus requested, boolean force) {
    if (force) {
      return requested;
    }
    if (previous == Removing) {
      return Removing;
    }

    if (previous == Stopped && requested == Unknown) {
      return Stopped;
    }
    return requested;
  }

  /**
   * Selects the reason for a local ReadOnly-to-ReadOnly update: Stopping > Manual >
   * UnrecoverableError > DiskFull > null or unclassified reasons. Equal priority keeps the previous
   * reason, including the first unrecoverable error's details.
   *
   * <p>Do not apply this priority to received heartbeat reasons, which already reflect the remote
   * node's decision.
   */
  public static String transitionReadOnlyReason(String previous, String requested) {
    return getReadOnlyReasonPriority(previous) >= getReadOnlyReasonPriority(requested)
        ? previous
        : requested;
  }

  private static int getReadOnlyReasonPriority(String reason) {
    if (reason == null) {
      return 0;
    }
    if (reason.startsWith(UNRECOVERABLE_ERROR)) {
      return 2;
    }
    return switch (reason) {
      case STOPPING -> 4;
      case MANUAL -> 3;
      case DISK_FULL -> 1;
      default -> 0;
    };
  }

  public static boolean isNormalStatus(NodeStatus status) {
    // Currently, the only normal status is Running
    return status != null && status.equals(NodeStatus.Running);
  }

  /**
   * Whether this status has a persistent record. Other statuses may still require clearing an
   * existing record when the node recovers.
   */
  public boolean isPersistentStatus() {
    return this == Stopped || this == Removing;
  }

  /** No available heartbeat, or an explicit shutdown report. Removing alone is not offline. */
  public boolean isOffline() {
    return this == Unknown || this == Stopped;
  }

  /** These statuses allow an offline node; they do not confirm that the node is offline. */
  public boolean mayBeOffline() {
    return this == Removing || isOffline();
  }

  /**
   * Eligible for a new or migrated Region replica. Offline nodes remain candidates when there are
   * too few online nodes; ReadOnly and Removing nodes must not receive new replicas.
   */
  public boolean isRegionCandidate() {
    return this == Running || isOffline();
  }

  public static boolean isReadable(NodeStatus status) {
    return !status.isOffline();
  }
}
