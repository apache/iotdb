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

package org.apache.iotdb.consensus.i18n;

/**
 * Ratis consensus specific messages. Log messages use SLF4J {@code {}} placeholders; exception
 * messages use {@code %s} (String.format) or plain strings.
 */
public final class RatisMessages {

  private RatisMessages() {}

  // ===================== RatisConsensus =====================

  public static final String INTERRUPTED_RETRYING_WRITE =
      "{}: interrupted when retrying for write request {}";
  public static final String NULL_REPLY_IN_WRITE_WITH_RETRY =
      "null reply received in writeWithRetry for request ";
  public static final String LEADER_READ_ONLY_STEP_DOWN_FAILED =
      "leader {} read only, force step down failed due to, ";
  public static final String TRY_ADD_CONFLICTING_PEER =
      "{}: try to add a peer {} with conflicting id or address in {}";
  public static final String IS_LEADER_REQUEST_FAILED =
      "isLeader request failed with exception: ";
  public static final String IS_LEADER_READY_REQUEST_FAILED =
      "isLeaderReady request failed with exception: ";
  public static final String GET_LOGICAL_CLOCK_REQUEST_FAILED =
      "getLogicalClock request failed with exception: ";
  public static final String IS_LEADER_READY_CHECKING_FAILED =
      "isLeaderReady checking failed with exception: ";
  public static final String LEADER_STILL_NOT_READY =
      "{}: leader is still not ready after {}ms";
  public static final String UNEXPECTED_INTERRUPTION_WAIT_LEADER_READY =
      "Unexpected interruption when waitUntilLeaderReady";
  public static final String FETCH_DIVISION_INFO_FAILED =
      "fetch division info for group {} failed due to: ";
  public static final String TRIGGER_SNAPSHOT_SUCCESS =
      "{} group {}: successfully taken snapshot at index {} with force = {}";
  public static final String GET_GROUP_FAILED =
      "get group {} failed ";
  public static final String BORROW_CLIENT_FROM_POOL_FAILED =
      "Borrow client from pool for group {} failed.";
  public static final String TRANSFER_LEADER_FAILED_TIMEOUT =
      "transferLeader for group %s to %s failed. This could be due to a timeout, "
          + "especially during heavy disk usage. Consider increasing the "
          + "'ratis_transfer_leader_timeout_ms' configuration property.";
  public static final String TRANSFER_LEADER_FAILED_STARTUP =
      "transferLeader for group %s to %s failed. This could be due to a timeout, "
          + "especially during initial startup. Consider increasing the "
          + "'ratis_rpc_transfer_leader_timeout_ms' configuration property.";

  // ===================== ApplicationStateMachineProxy =====================

  public static final String STATEMACHINE_RUNTIME_EXCEPTION =
      "application statemachine throws a runtime exception: ";
  public static final String INTERNAL_ERROR_STATEMACHINE_RUNTIME_EXCEPTION =
      "internal error. statemachine throws a runtime exception: ";
  public static final String INTERRUPTED_WAITING_SYSTEM_READY =
      "{}: interrupted when waiting until system ready: ";
  public static final String REQUEST_MESSAGE_REQUIRED =
      "An RequestMessage is required but got {}";
  public static final String UNABLE_TO_CREATE_TEMP_SNAPSHOT_DIR =
      "Unable to create temp snapshotDir at {}";
  public static final String ATOMIC_RENAME_FAILED =
      "{} atomic rename {} to {} failed with exception {}";
  public static final String SNAPSHOT_DIR_INCOMPLETE_DELETING =
      "Snapshot directory is incomplete, deleting {}";

  // ===================== RatisClient =====================

  public static final String CANNOT_CLOSE_RAFT_CLIENT =
      "cannot close raft client ";
  public static final String RAFT_CLIENT_REQUEST_FAILED =
      "{}: raft client request failed and caught exception: ";

  // ===================== DiskGuardian =====================

  public static final String ERROR_LISTING_FILES =
      "{}: Error caught when listing files for {} at {}:";
  public static final String CLEAR_SNAPSHOT_FLAG_FAILED =
      "{}: clear snapshot flag failed for group {}, please check the related implementation";
  public static final String TAKE_SNAPSHOT_FAILED =
      "{} take snapshot failed for group {} due to {}. Disk file status {}";

  // ===================== SnapshotStorage =====================

  public static final String CANNOT_CONSTRUCT_SNAPSHOT_DIR_STREAM =
      "Cannot construct snapshot directory stream ";
  public static final String CANNOT_RESOLVE_REAL_PATH =
      "{} cannot resolve real path of {} due to ";

  // ===================== ResponseMessage =====================

  public static final String SERIALIZE_TSSTATUS_FAILED =
      "serialize TSStatus failed {}";

  // ===================== MetricRegistryManager =====================

  public static final String REPORTER_DISABLED =
      "Reporter is disabled from RatisMetricRegistries";
  public static final String JMX_REPORTER_DISABLED =
      "JMX Reporter is disabled from RatisMetricRegistries";
  public static final String CONSOLE_REPORTER_DISABLED =
      "Console Reporter is disabled from RatisMetricRegistries";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_RESET_PEER_LIST_CURRENT_PEER_LIST_CORRECT_NOTHING_NEED_RESET_0E009CDA = "[RESET PEER LIST] The current peer list is correct, nothing need to be reset: {}";
  public static final String EXCEPTION_INTERNAL_GRPC_CONNECTION_ERROR_59404D15 = "internal GRPC connection error:";
  public static final String LOG_FAILED_ARG_ATTEMPT_ARG_SLEEP_ARG_THEN_RETRY_36A9A0C2 = "Failed {}, attempt #{}, sleep {} and then retry";
  public static final String LOG_ARG_INTERRUPTED_WAITING_RETRY_38D69BCC = "{}: interrupted when waiting for retry";
  public static final String EXCEPTION_SUPPLIER_EQUALS_EQUALS_NULL_13BACC3E = "supplier == null";
  public static final String EXCEPTION_CONDITION_EQUALS_EQUALS_NULL_E3A6C947 = "condition == null";
  public static final String EXCEPTION_ARG_FAILED_TO_LOAD_SNAPSHOT_FROM_ARG_A12E23D7 =
      "%s: failed to load snapshot from %s";

}
