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
 * IoTConsensusV2 (pipe-based consensus) specific messages. Log messages use SLF4J {@code {}}
 * placeholders; exception messages use {@code %s} (String.format) or plain strings.
 */
public final class IoTConsensusV2Messages {

  private IoTConsensusV2Messages() {}

  // ===================== IoTConsensusV2 lifecycle =====================

  public static final String RECOVER_TASK_CANCELLED =
      "IoTV2 Recover Task is cancelled";
  public static final String RECOVER_FUTURE_EXCEPTION =
      "Exception while waiting for recover future completion";
  public static final String RECOVER_TASK_INTERRUPTED =
      "IoTV2 Recover Task is interrupted";
  public static final String FAILED_RECOVER_CONSENSUS =
      "Failed to recover consensus from {} for {}, ignore it and continue recover other group, async backend checker thread will automatically deregister related pipe side effects for this failed consensus group.";
  public static final String FAILED_RECOVER_CONSENSUS_READ_DIR =
      "Failed to recover consensus from {} because read dir failed";
  public static final String FAILED_RECOVER_CONSENSUS_SHORT =
      "Failed to recover consensus from {}";

  // ===================== IoTConsensusV2 peer operations =====================

  public static final String START_DELETE_LOCAL_PEER =
      "[{}] start to delete local peer for group {}";
  public static final String FINISH_DELETE_LOCAL_PEER =
      "[{}] finish deleting local peer for group {}";
  public static final String INACTIVATE_NEW_PEER =
      "[{}] inactivate new peer: {}";
  public static final String NOTIFY_CREATE_CONSENSUS_PIPES =
      "[{}] notify current peers to create consensus pipes...";
  public static final String WAIT_PEERS_FINISH_TRANSFER =
      "[{}] wait until all the other peers finish transferring...";
  public static final String ACTIVATE_NEW_PEER =
      "[{}] activate new peer...";
  public static final String ADD_REMOTE_PEER_FAILED_CLEANUP =
      "[{}] add remote peer failed, automatic cleanup side effects...";
  public static final String FAILED_CLEANUP_SIDE_EFFECTS =
      "[{}] failed to cleanup side effects after failed to add remote peer";
  public static final String NOTIFY_DROP_CONSENSUS_PIPES =
      "[{}] notify other peers to drop consensus pipes...";
  public static final String INACTIVATE_PEER =
      "[{}] inactivate peer {}";
  public static final String WAIT_TARGET_PEER_COMPLETE_TRANSFER =
      "[{}] wait target peer{} complete transfer...";
  public static final String WAIT_PEER_RELEASE_RESOURCE =
      "[{}] wait {} to release all resource...";
  public static final String NOT_SUPPORT_LEADER_TRANSFER =
      "%s does not support leader transfer";

  // ===================== IoTConsensusV2ServerImpl =====================

  public static final String ERROR_SET_PEER_ACTIVE =
      "error when set peer %s to active %s. result status: %s";
  public static final String ERROR_SET_PEER_ACTIVE_SHORT =
      "error when set peer %s to active %s";
  public static final String TARGET_PEER_MAY_BE_DOWN =
      "target peer may be down, error when set peer {} to active {}";
  public static final String CANNOT_NOTIFY_PEER_CREATE_PIPE =
      "{} cannot notify peer {} to create consensus pipe, may because that peer is unknown currently, please manually check!";
  public static final String CANNOT_CREATE_CONSENSUS_PIPE =
      "{} cannot create consensus pipe to {}, may because target peer is unknown currently, please manually check!";
  public static final String ERROR_NOTIFY_PEER_CREATE_PIPE =
      "error when notify peer %s to create consensus pipe";
  public static final String CANNOT_NOTIFY_PEER_DROP_PIPE =
      "{} cannot notify peer {} to drop consensus pipe, may because that peer is unknown currently, please manually check!";
  public static final String CANNOT_DROP_CONSENSUS_PIPE =
      "{} cannot drop consensus pipe to {}, may because target peer is unknown currently, please manually check!";
  public static final String ERROR_NOTIFY_PEER_DROP_PIPE =
      "error when notify peer %s to drop consensus pipe";
  public static final String INTERRUPTED_WAITING_TRANSFER =
      "{} is interrupted when waiting for transfer completed";
  public static final String INTERRUPTED_WAITING_TRANSFER_FMT =
      "%s is interrupted when waiting for transfer completed";
  public static final String CANNOT_CHECK_PIPE_TRANSMISSION =
      "{} cannot check consensus pipes transmission completed to peer {}";
  public static final String ERROR_CHECK_PIPE_TRANSMISSION =
      "error when check consensus pipes transmission completed to peer %s";
  public static final String CANNOT_CHECK_PIPE_TRANSMISSION_SHORT =
      "{} cannot check consensus pipes transmission completed";

  // ===================== IoTConsensusV2RPCServiceProcessor =====================

  public static final String UNEXPECTED_GROUP_SET_ACTIVE =
      "unexpected consensusGroupId %s for set active request %s";
  public static final String UNEXPECTED_GROUP_CREATE_PIPE =
      "unexpected consensusGroupId %s for create consensus pipe request %s";
  public static final String UNEXPECTED_GROUP_DROP_PIPE =
      "unexpected consensusGroupId %s for drop consensus pipe request %s";
  public static final String UNEXPECTED_GROUP_CHECK_TRANSFER =
      "unexpected consensusGroupId %s for check transfer completed request %s";
  public static final String UNEXPECTED_GROUP_WAIT_RELEASE =
      "unexpected consensusGroupId %s for TWaitReleaseAllRegionRelatedResourceRes request";
  public static final String FAILED_CREATE_CONSENSUS_PIPE =
      "Failed to create consensus pipe to target peer with req {}";
  public static final String FAILED_DROP_CONSENSUS_PIPE =
      "Failed to drop consensus pipe to target peer with req {}";
  public static final String FAILED_CHECK_CONSENSUS_PIPE =
      "Failed to check consensus pipe completed with req {}, set is completed to {}";
}
