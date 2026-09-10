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
 * Shared / common consensus messages used across all consensus implementations. Log messages use
 * SLF4J {@code {}} placeholders; exception messages use {@code %s} (String.format) or plain
 * strings.
 */
public final class ConsensusMessages {

  private ConsensusMessages() {}

  // ===================== ConsensusFactory =====================

  public static final String CONSTRUCT_FAILED_MSG =
      "Construct consensusImpl failed, Please check your consensus className %s";
  public static final String COULD_NOT_CONSTRUCT_ICONSENSUS =
      "Couldn't Construct IConsensus class: {}";
  public static final String UTILITY_CLASS_CONSENSUS_FACTORY = "Utility class ConsensusFactory";

  // ===================== Exception messages (String.format %s) =====================

  public static final String CONSENSUS_GROUP_NOT_EXIST =
      "The consensus group %s doesn't exist";
  public static final String CONSENSUS_GROUP_ALREADY_EXIST =
      "The consensus group %d already exists";
  public static final String ILLEGAL_PEER_NUM =
      "Illegal peer num %d when adding consensus group";
  public static final String ILLEGAL_PEER_ENDPOINT =
      "Illegal addConsensusGroup because currentNode %s is not in consensusGroup %s";
  public static final String PEER_ALREADY_IN_GROUP =
      "Peer %s:%d is already in group %d";
  public static final String PEER_NOT_IN_GROUP =
      "Peer %s is not in group %d";

  // ===================== Common log messages (SLF4J {}) =====================

  public static final String UNABLE_TO_CREATE_CONSENSUS_DIR =
      "Unable to create consensus dir at {}";
  public static final String UNABLE_TO_CREATE_CONSENSUS_DIR_FMT =
      "Unable to create consensus dir at %s";
  public static final String UNABLE_TO_CREATE_CONSENSUS_DIR_FOR_GROUP =
      "Unable to create consensus dir for group {} at {}";
  public static final String UNABLE_TO_CREATE_CONSENSUS_DIR_FOR_GROUP_FMT =
      "Unable to create consensus dir for group %s";
  public static final String CANNOT_CREATE_LOCAL_PEER =
      "Cannot create local peer for group {} with peers {}";
  public static final String FAILED_TO_RESET_PEER_LIST_WHILE_START =
      "Failed to reset peer list while start";
  public static final String RECORD_CORRECT_PEER_LIST =
      "Record correct peer list: {}";
  public static final String INTERRUPTED_WHEN_SHUTTING_DOWN_EXECUTOR =
      "{}: interrupted when shutting down add Executor with exception {}";
  public static final String INTERRUPTED_WHEN_SHUTTING_DOWN_EXECUTOR_RATIS =
      "{}: interrupted when shutting down add Executor with exception ";
  public static final String SET_ACTIVE_STATUS = "set {} active status to {}";

  // ===================== Peer reset log messages (SLF4J {}) =====================

  public static final String RESET_PEER_LIST_NOT_IN_CORRECT =
      "[RESET PEER LIST] {} Local peer is not in the correct configuration, delete it.";
  public static final String RESET_PEER_LIST_DELETE_LOCAL_PEER =
      "[RESET PEER LIST] Local peer is not in the correct peer list, delete local peer {}";
  public static final String RESET_PEER_LIST_REMOVE_SYNC_CHANNEL =
      "[RESET PEER LIST] {} Remove sync channel with: {}";
  public static final String RESET_PEER_LIST_FAILED_TO_REMOVE_SYNC_CHANNEL =
      "[RESET PEER LIST] {} Failed to remove sync channel with: {}";
  public static final String RESET_PEER_LIST_BUILD_SYNC_CHANNEL =
      "[RESET PEER LIST] {} Build sync channel with: {}";
  public static final String RESET_PEER_LIST_FAILED_TO_BUILD_SYNC_CHANNEL =
      "[RESET PEER LIST] {} Failed to build sync channel with: {}";
  public static final String RESET_PEER_LIST_RESET_RESULT =
      "[RESET PEER LIST] {} Local peer list has been reset: {} -> {}";
  public static final String RESET_PEER_LIST_NOTHING_TO_RESET =
      "[RESET PEER LIST] {} The current peer list is correct, nothing need to be reset: {}";
  public static final String RESET_PEER_LIST_WILL_RESET =
      "[RESET PEER LIST] Peer list will be reset from {} to {}";
  public static final String RESET_PEER_LIST_RESET_SUCCESS =
      "[RESET PEER LIST] Peer list has been reset to {}";
  public static final String RESET_PEER_LIST_RESET_FAILED =
      "[RESET PEER LIST] Peer list failed to reset to {}, reply is {}";

  // ===================== SimpleConsensus messages =====================

  public static final String SIMPLE_CONSENSUS_NOT_SUPPORT_MEMBERSHIP_CHANGES =
      "SimpleConsensus does not support membership changes";
  public static final String SIMPLE_CONSENSUS_NOT_SUPPORT_LEADER_TRANSFER =
      "SimpleConsensus does not support leader transfer";
  public static final String SIMPLE_CONSENSUS_NOT_SUPPORT_SNAPSHOT_TRIGGER =
      "SimpleConsensus does not support snapshot trigger currently";
  public static final String SIMPLE_CONSENSUS_NOT_SUPPORT_RESET_PEER_LIST =
      "SimpleConsensus does not support reset peer list";
  public static final String SIMPLE_CONSENSUS_NOOP_RECORD_PEER_LIST =
      "SimpleConsensus will do nothing when calling recordCorrectPeerListBeforeStarting";

  // ===================== RPC processor common messages =====================

  public static final String UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST =
      "unexpected consensusGroupId %s for %s request";
  public static final String UNEXPECTED_CONSENSUS_GROUP_ID_FOR_SYNC_LOG =
      "unexpected consensusGroupId %s for TSyncLogEntriesReq which size is %s";
  public static final String SYNC_LOG_SYSTEM_READ_ONLY =
      "fail to sync logEntries because system is read-only.";
  public static final String PEER_INACTIVE_NOT_READY =
      "Peer is inactive and not ready to receive sync log request, %s, DataNode Id: %s";
  public static final String PEER_INACTIVE_NOT_READY_WRITE =
      "Peer is inactive and not ready to write request, %s, DataNode Id: %s";
  public static final String REMOVE_SYNC_LOG_CHANNEL_FAILED =
      "remove sync log channel failed";
  public static final String FAILED_TO_CLEANUP_TRANSFERRED_SNAPSHOT =
      "failed to cleanup transferred snapshot {}";

  // ===================== Wait release resource messages =====================

  public static final String WAIT_RELEASE_HAS_RELEASED =
      "[WAIT RELEASE] {} has released all region related resource";
  public static final String WAIT_RELEASE_STILL_RELEASING =
      "[WAIT RELEASE] {} is still releasing all region related resource";
  public static final String ERROR_WAITING_RELEASE_RESOURCE =
      "error when waiting %s to release all region related resource. %s";
  public static final String THREAD_INTERRUPTED_WAITING_RELEASE_RESOURCE =
      "thread interrupted when waiting %s to release all region related resource. %s";

  // ===================== Duplicate peer warning =====================

  public static final String DUPLICATE_PEERS_IGNORED =
      "Duplicate peers in the input list, ignore the duplicates.";

  // ===================== Consensus pipe name =====================

  public static final String INVALID_PIPE_NAME = "Invalid pipe name: ";

  // ===================== Not active write reject =====================

  public static final String NODE_NOT_ACTIVE_REJECT_WRITE =
      "current node is not active and is not ready to receive user write.";

  // ===================== Utility messages =====================

  public static final String FAILED_TO_SERIALIZE_PEER = "Failed to serialize Peer";
  public static final String VISIT_FILE_FAILED = "visit file {} failed due to {}";
  public static final String IO_EXCEPTION_LISTING_SNAPSHOT_DIR =
      "IOException occurred during listing snapshot directory: ";
  public static final String FAILED_TO_LOAD_KEYSTORE =
      "Failed or truststore to load keystore file";
  public static final String KEYSTORE_FILE_NOT_FOUND = "keystore or truststore file not found";
  public static final String FAILED_TO_READ_KEYSTORE =
      "Failed to read key store or trust store.";
  public static final String NOT_IMPLEMENTED_YET = "not implemented yet";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_RAFT_SERVER_CANNOT_SERVE_READ_REQUESTS_NOW_LEADER_UNKNOWN_UNDER_B6D65373 = "Raft Server cannot serve read requests now (leader is unknown or under recovery). ";
  public static final String EXCEPTION_PLEASE_TRY_READ_LATER_D8E0CDE1 = "Please try read later: ";
  public static final String EXCEPTION_RATIS_REQUEST_FAILED_52AF217F = "Ratis request failed ";
  public static final String EXCEPTION_UNKNOWN_88183B94 = "Unknown";
  public static final String EXCEPTION_RATIS_REQUEST_FAILED_58107CDE = "Ratis request failed: ";
  public static final String EXCEPTION_DOT_F779BA66 = ". ";

}
