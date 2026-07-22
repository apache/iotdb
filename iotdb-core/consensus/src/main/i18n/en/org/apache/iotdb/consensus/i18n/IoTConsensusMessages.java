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
 * IoTConsensus (v1) specific messages. Log messages use SLF4J {@code {}} placeholders; exception
 * messages use {@code %s} (String.format) or plain strings.
 */
public final class IoTConsensusMessages {

  private IoTConsensusMessages() {}

  // ===================== IoTConsensus lifecycle =====================

  public static final String INACTIVATE_NEW_PEER =
      "[IoTConsensus] inactivate new peer: {}";
  public static final String NOTIFY_PEERS_BUILD_SYNC_LOG =
      "[IoTConsensus] notify current peers to build sync log...";
  public static final String START_TAKE_SNAPSHOT =
      "[IoTConsensus] start to take snapshot...";
  public static final String START_TRANSMIT_SNAPSHOT =
      "[IoTConsensus] start to transmit snapshot...";
  public static final String TRIGGER_LOAD_SNAPSHOT =
      "[IoTConsensus] trigger new peer to load snapshot...";
  public static final String ACTIVATE_NEW_PEER =
      "[IoTConsensus] activate new peer...";
  public static final String CLEANUP_REMOTE_SNAPSHOT =
      "[IoTConsensus] clean up remote snapshot...";
  public static final String FAILED_CLEANUP_REMOTE_SNAPSHOT =
      "[IoTConsensus] failed to cleanup remote snapshot";
  public static final String ADD_REMOTE_PEER_FAILED_CLEANUP =
      "[IoTConsensus] add remote peer failed, automatic cleanup side effects...";
  public static final String CLEANUP_LOCAL_SNAPSHOT =
      "[IoTConsensus] clean up local snapshot...";
  public static final String NOT_SUPPORT_LEADER_TRANSFER =
      "IoTConsensus does not support leader transfer";

  // ===================== IoTConsensusServerImpl =====================

  public static final String THROTTLE_DOWN = "[Throttle Down] index:{}, safeIndex:{}";
  public static final String DATA_REGION_INDEX_AFTER_BUILD =
      "DataRegion[{}]: index after build: safeIndex:{}, searchIndex: {}, lastConsensusRequest: {}";
  public static final String FAILED_TO_THROTTLE_DOWN = "Failed to throttle down because ";

  // ===================== Snapshot =====================

  public static final String CANNOT_MKDIR_FOR_SNAPSHOT =
      "%s: cannot mkdir for snapshot";
  public static final String UNKNOWN_ERROR_TAKING_SNAPSHOT =
      "unknown error when taking snapshot";
  public static final String ERROR_TAKING_SNAPSHOT =
      "error when taking snapshot";
  public static final String CANNOT_FIND_SNAPSHOT_DIR =
      "Can not find any snapshot dir after build a new snapshot for group {}";
  public static final String DELETE_OLD_SNAPSHOT_FAILED =
      "Delete old snapshot dir {} failed";
  public static final String FILE_NOT_EXIST = "File not exist: {}";
  public static final String CLEANUP_LOCAL_SNAPSHOT_FAIL =
      "Cleanup local snapshot fail. You may manually delete {}.";
  public static final String INVALID_SNAPSHOT_FILE =
      "invalid snapshot file. snapshotId: %s, filePath: %s";
  public static final String ERROR_RECEIVING_SNAPSHOT =
      "error when receiving snapshot %s";
  public static final String INVALID_SNAPSHOT_RELATIVE_PATH =
      "Invalid snapshotRelativePath: ";

  // ===================== Snapshot transmission =====================

  public static final String SNAPSHOT_TRANSMISSION_START =
      "[SNAPSHOT TRANSMISSION] Start to transmit snapshots ({} files, total size {}) from dir {}";
  public static final String SNAPSHOT_TRANSMISSION_ALL_FILES =
      "[SNAPSHOT TRANSMISSION] All the files below shell be transmitted: {}";
  public static final String SNAPSHOT_TRANSMISSION_ERROR =
      "[SNAPSHOT TRANSMISSION] Error when transmitting snapshot fragment to %s";
  public static final String SNAPSHOT_TRANSMISSION_PROGRESS =
      "[SNAPSHOT TRANSMISSION] The overall progress for dir {}: files {}/{} done, size {}/{} done, time {} passed. File {} done.";
  public static final String SNAPSHOT_TRANSMISSION_SEND_ERROR =
      "[SNAPSHOT TRANSMISSION] Error when send snapshot file to %s";
  public static final String SNAPSHOT_TRANSMISSION_COMPLETE =
      "[SNAPSHOT TRANSMISSION] After {}, successfully transmit all snapshots from dir {}";

  // ===================== Peer operations =====================

  public static final String ERROR_INACTIVATING_PEER =
      "error when inactivating %s. %s";
  public static final String ERROR_INACTIVATING_PEER_SHORT =
      "error when inactivating %s";
  public static final String ERROR_TRIGGERING_SNAPSHOT_LOAD =
      "error when triggering snapshot load %s. %s";
  public static final String ERROR_ACTIVATING_PEER =
      "error when activating %s. %s";
  public static final String ERROR_ACTIVATING_PEER_SHORT =
      "error when activating %s";
  public static final String CLEANUP_REMOTE_SNAPSHOT_FAILED =
      "cleanup remote snapshot failed of %s ,status is %s";
  public static final String CLEANUP_REMOTE_SNAPSHOT_FAILED_SHORT =
      "cleanup remote snapshot failed of %s";

  // ===================== Sync log =====================

  public static final String NOTIFY_PEERS_BUILD_SYNC_LOG_DETAIL =
      "[IoTConsensus] notify current peers to build sync log. group member: {}, target: {}";
  public static final String BUILD_SYNC_LOG_CHANNEL_FROM =
      "[IoTConsensus] build sync log channel from {}";
  public static final String BUILD_SYNC_LOG_CHANNEL_FAILED =
      "build sync log channel failed from %s to %s";
  public static final String CANNOT_NOTIFY_BUILD_SYNC_LOG =
      "cannot notify {} to build sync log channel. Please check the status of this node manually";
  public static final String BUILD_SYNC_LOG_CHANNEL_SUCCESS =
      "[IoTConsensus] Successfully build sync log channel to {} with initialSyncIndex {}. {}";
  public static final String SYNC_LOG_CHANNEL_STARTED =
      "Sync log channel has started.";
  public static final String SYNC_LOG_CHANNEL_START_LATER =
      "Sync log channel maybe start later.";
  public static final String REMOVING_SYNC_LOG_CHANNEL_FAILED =
      "removing sync log channel failed from {} to {}";
  public static final String EXCEPTION_REMOVING_SYNC_LOG_CHANNEL =
      "Exception happened during removing sync log channel from {} to {}";
  public static final String LOG_DISPATCHER_REMOVED_CLEANUP =
      "[IoTConsensus] log dispatcher to {} removed and cleanup";
  public static final String EXCEPTION_REMOVING_LOG_DISPATCHER =
      "[IoTConsensus] Exception happened during removing log dispatcher thread, but configuration.dat will still be removed.";
  public static final String SUGGEST_RESTART_DATANODE =
      "It's suggested restart the DataNode to remove log dispatcher thread.";
  public static final String LOG_DISPATCHER_REMOVED_AND_CLEANUP =
      "[IoTConsensus] Log dispatcher thread to {} has been removed and cleanup";
  public static final String CONFIGURATION_UPDATED =
      "[IoTConsensus Configuration] Configuration updated to {}. {}";

  // ===================== Wait sync log =====================

  public static final String WAIT_SYNC_LOG_COMPLETED =
      "[WAIT LOG SYNC] {} SyncLog is completed. TargetIndex: {}, CurrentSyncIndex: {}";
  public static final String WAIT_SYNC_LOG_IN_PROGRESS =
      "[WAIT LOG SYNC] {} SyncLog is still in progress. TargetIndex: {}, CurrentSyncIndex: {}";
  public static final String ERROR_WAITING_SYNC_LOG_COMPLETE =
      "error when waiting %s to complete SyncLog. %s";
  public static final String THREAD_INTERRUPTED_WAITING_SYNC_LOG =
      "thread interrupted when waiting %s to complete SyncLog. %s";

  // ===================== Index controller =====================

  public static final String UPDATE_INDEX =
      "update index from currentIndex {} to {} for file prefix {} in {}";
  public static final String VERSION_FILE_UPDATED =
      "version file updated, previous: {}, current: {}";
  public static final String FAILED_FLUSH_SYNC_INDEX =
      "failed to flush sync index because previous version file {} does not exists. "
          + "It may be caused by the target Peer is removed from current group. "
          + "target file is {}";
  public static final String ERROR_FLUSHING_NEXT_VERSION =
      "Error occurred when flushing next version";
  public static final String VERSION_FILE_UPGRADE =
      "version file upgrade, previous: {}, current: {}";
  public static final String ERROR_UPGRADING_VERSION_FILE =
      "Error occurred when upgrading version file";
  public static final String DELETE_OUTDATED_VERSION_FILE_FAILED =
      "Delete outdated version file {} failed";
  public static final String ERROR_CREATING_NEW_FILE =
      "Error occurred when creating new file {}";
  public static final String CONFIGURATION_EMPTY_UNEXPECTED =
      "Configuration is empty, which is unexpected. Safe deleted search index won't be updated this time.";
  public static final String SEARCH_INDEX_SMALLER_THAN_SAFELY_DELETED =
      "The searchIndex for this region({}) is smaller than the safelyDeletedSearchIndex when "
          + "the node is restarted, which means that the data of the current region is not flushed "
          + "by the wal, but has been synchronized to other nodes. At this point, "
          + "different replicas have been inconsistent and cannot be automatically recovered. "
          + "To prevent subsequent logs from marking smaller searchIndex and exacerbating the "
          + "inconsistency, we manually set the searchIndex({}) to safelyDeletedSearchIndex({}) "
          + "here to reduce the impact of this problem in the future";

  // ===================== LogDispatcher =====================

  public static final String UNABLE_TO_SHUTDOWN_LOG_DISPATCHER =
      "Unable to shutdown LogDispatcher service after {} seconds";
  public static final String UNEXPECTED_INTERRUPTION_CLOSING_LOG_DISPATCHER =
      "Unexpected Interruption when closing LogDispatcher service ";
  public static final String DISPATCHER_STARTS =
      "{}: Dispatcher for {} starts";
  public static final String DISPATCHER_EXITS =
      "{}: Dispatcher for {} exits";
  public static final String DISPATCHER_DID_NOT_STOP =
      "{}: Dispatcher for {} didn't stop after 30s.";
  public static final String UNEXPECTED_ERROR_IN_LOG_DISPATCHER =
      "Unexpected error in logDispatcher for peer {}";
  public static final String PUSH_LOG_TO_QUEUE =
      "{}->{}: Push a log to the queue, where the queue length is {}";
  public static final String LOG_QUEUE_FULL =
      "{}: Log queue of {} is full, ignore the log to this node, searchIndex: {}";
  public static final String GET_BATCH_START_INDEX =
      "{}: startIndex: {}, maxIndex: {}, pendingEntries size: {}, bufferedEntries size: {}";
  public static final String ACCUMULATED_FROM_WAL_WHEN_EMPTY =
      "{} : accumulated a {} from wal when empty";
  public static final String ACCUMULATED_FROM_WAL =
      "{} : accumulated a {} from wal";
  public static final String ACCUMULATED_FROM_QUEUE =
      "{} : accumulated a {} from queue";
  public static final String ACCUMULATED_FROM_QUEUE_AND_WAL_GAP =
      "gap {} : accumulated a {} from queue and wal when gap";
  public static final String ACCUMULATED_FROM_QUEUE_AND_WAL =
      "{} : accumulated a {} from queue and wal";
  public static final String SEND_BATCH =
      "Send Batch[startIndex:{}, endIndex:{}] to ConsensusGroup:{}";
  public static final String CANNOT_SYNC_LOGS_TO_PEER =
      "Can not sync logs to peer {} because";
  public static final String CONSTRUCT_FROM_WAL =
      "construct from WAL for one Entry, index : {}";
  public static final String WAIT_NEXT_WAL_INTERRUPTED =
      "wait for next WAL entry is interrupted";
  public static final String SEARCH_ENTRY_FOUND_SMALLER =
      "search for one Entry which index is {}, but find a smaller one, index : {}";
  public static final String SEARCH_ENTRY_FOUND_LARGER =
      "search for one Entry which index is {}, but find a larger one, index : {}."
          + "Perhaps the wal file is corrupted, in which case we skip it and choose a larger index to replicate";
  public static final String DATA_REGION_CONSTRUCT_FROM_WAL =
      "DataRegion[{}]->{}: currentIndex: {}, maxIndex: {}";

  // ===================== DispatchLogHandler =====================

  public static final String CANNOT_SEND_TO_PEER =
      "Can not send {} to peer {} for {} times because {}";
  public static final String SEND_COMPLETE_BUT_CONTAINS_ERROR =
      "Send {} to peer {} complete but contains unsuccessful status: {}";
  public static final String CANNOT_SEND_TO_PEER_ON_ERROR =
      "Can not send {} to peer for {} times {} because {}";
  public static final String SKIP_RETRY_TAPPLICATION_EXCEPTION =
      "Skip retrying this Batch {} because of TApplicationException.";
  public static final String LOG_DISPATCHER_STOPPED_NO_RETRY =
      "LogDispatcherThread {} has been stopped, "
          + "we will not retrying this Batch {} after {} times";

  // ===================== SyncLogCacheQueue =====================

  public static final String CACHE_AND_INSERT_START =
      "cacheAndInsert start: source = {}, region = {}, queue size {}, startSyncIndex = {}, endSyncIndex = {}";
  public static final String CACHE_AND_INSERT_END =
      "cacheAndInsert end: source = {}, region = {}, queue size {}, startSyncIndex = {}, endSyncIndex = {}, sortTime = {}ms, applyTime = {}ms";
  public static final String WAITING_TARGET_REQUEST_TIMEOUT =
      "waiting target request timeout. current index: {}, target index: {}";
  public static final String CURRENT_WAITING_INTERRUPTED =
      "current waiting is interrupted. SyncIndex: {}. Exception: ";

  // ===================== SyncStatus =====================

  public static final String SYNC_STATUS_OFFER =
      "Offer Batch[startIndex:{}, endIndex:{}] to SyncStatus. "
          + "Current size of SyncStatus: {}. Pending Size: {}";

  // ===================== AsyncClient =====================

  public static final String UNEXPECTED_EXCEPTION_IN_CLIENT =
      "Unexpected exception occurs in {}, error msg is {}";
  public static final String CLIENT_INVALIDATED = "This client has been invalidated";

  // ===================== RPC Processor execute log sync =====================

  public static final String EXECUTE_SYNC_LOG_ENTRIES =
      "execute TSyncLogEntriesReq for {} with result {}";

  // ===================== Memory Manager =====================

  public static final String RESERVING_BYTES_FOR_REQUEST_SUCCEEDS =
      "Reserving {} bytes for request {} succeeds, current total usage {}";
  public static final String RESERVING_BYTES_FOR_REQUEST_FAILS =
      "Reserving {} bytes for request {} fails, current total usage {}";
  public static final String SKIP_MEMORY_RESERVATION =
      "Skip memory reservation for {} because its ref count is not 0";
  public static final String RESERVING_BYTES_FOR_BATCH_SUCCEEDS =
      "Reserving {} bytes for batch {}-{} succeeds, current total usage {}";
  public static final String RESERVING_BYTES_FOR_BATCH_FAILS =
      "Reserving {} bytes for batch {}-{} fails, current total usage {}";
  public static final String FREED_BYTES_FOR_REQUEST =
      "Freed {} bytes for request {}, current total usage {}";
  public static final String FREED_BYTES_FOR_BATCH =
      "Freed {} bytes for batch {}-{}, current total usage {}";
  public static final String FREE_MEMORY =
      "{} free {} bytes, total memory size: {} bytes.";
  public static final String INTERRUPTED_AFTER_POLLING_AND_SLEEPING =
      "Interrupted after polling and sleeping";
  public static final String INTERRUPTED_AFTER_GETTING_A_BATCH =
      "Interrupted after getting a batch";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_ONNEWPEERCREATED_CALLBACK_FAILED_GROUP_ARG_2671FCDA = "onNewPeerCreated callback failed for group {}";
  public static final String LOG_ONPEERREMOVED_CALLBACK_FAILED_GROUP_ARG_9B79CBAF = "onPeerRemoved callback failed for group {}";
  public static final String EXCEPTION_UNSUPPORTED_WRITER_META_VERSION_ARG_ARG_7D14E679 = "Unsupported writer meta version %d in %s";
  public static final String LOG_WRITE_NO_SUBSCRIPTION_QUEUES_REGISTERED_0F4E697B = "write() no subscription queues registered, ";
  public static final String LOG_GROUP_ARG_SEARCHINDEX_ARG_ARG_D5E034E6 = "group={}, searchIndex={}, this={}";
  public static final String LOG_CANNOT_NOTIFY_ARG_BUILD_SYNC_LOG_CHANNEL_490770FB = "cannot notify {} to build sync log channel. ";
  public static final String LOG_PLEASE_CHECK_STATUS_NODE_MANUALLY_40FBC9B3 = "Please check the status of this node manually";
  public static final String LOG_FAILED_SYNC_WRITER_SAFE_TIME_BARRIER_PEER_ARG_GROUP_ARG_05A13E6A = "Failed to sync writer safe-time barrier to peer {} for group {}, ";
  public static final String LOG_SAFEPT_ARG_WRITERNODEID_ARG_BARRIER_ARG_AC74618F = "safePt={}, writerNodeId={}, barrier={}";
  public static final String LOG_RECOVERED_WRITER_META_GROUP_ARG_ARG_RECOVEREDLOCALSEQ_ARG_9B989AAC = "Recovered writer meta for group {} from {}, recoveredLocalSeq={}, ";
  public static final String LOG_PERSISTEDLOCALSEQ_ARG_5EBAA9A5 = "persistedLocalSeq={}";
  public static final String LOG_FAILED_LOAD_WRITER_META_GROUP_ARG_ARG_STARTING_FRESH_WRITER_A24EDEFE = "Failed to load writer meta for group {} from {}. Starting fresh writer metadata.";
  public static final String LOG_INITIALIZED_FRESH_WRITER_META_GROUP_ARG_RECOVEREDLOCALSEQ_ARG_A7254C6E = "Initialized fresh writer meta for group {}, recoveredLocalSeq={}";
  public static final String LOG_FAILED_PERSIST_WRITER_META_GROUP_ARG_AT_LOCALSEQ_ARG_PT_3502F119 = "Failed to persist writer meta for group {} at localSeq={}, pt={}";
  public static final String LOG_REGISTERED_SUBSCRIPTION_QUEUE_GROUP_ARG_5102ABA0 = "Registered subscription queue for group {}, ";
  public static final String LOG_TOTAL_SUBSCRIPTION_QUEUES_ARG_CURRENTSEARCHINDEX_ARG_ARG_9BF9006A = "total subscription queues: {}, currentSearchIndex={}, this={}";
  public static final String LOG_DEREGISTERED_SUBSCRIPTION_QUEUE_GROUP_ARG_REMAINING_SUBSCRIPTION_QUEUES_ARG_B86E31AF = "Deregistered subscription queue for group {}, remaining subscription queues: {}";
  public static final String LOG_OBSERVED_INCOMPARABLE_WRITER_SAFE_TIME_BARRIER_WRITER_ARG_0F0C171D = "Observed incomparable writer safe-time barrier for writer {}. ";
  public static final String LOG_KEEP_PENDINGSAFEPHYSICALTIME_ARG_PENDINGSAFELOCALSEQ_ARG_7123FD95 = "keep pendingSafePhysicalTime={}, pendingSafeLocalSeq={}, ";
  public static final String LOG_IGNORE_SAFEPHYSICALTIME_ARG_SAFELOCALSEQ_ARG_A601C4D1 = "ignore safePhysicalTime={}, safeLocalSeq={}";
  public static final String LOG_WRITE_OFFERING_ARG_SUBSCRIPTION_QUEUE_S_GROUP_ARG_SEARCHINDEX_ARG_A8489EDF = "write() offering to {} subscription queue(s), group={}, searchIndex={}, requestType={}";
  public static final String LOG_OFFER_RESULT_ARG_QUEUESIZE_ARG_QUEUEREMAINING_ARG_7ADC84C2 = "offer result={}, queueSize={}, queueRemaining={}";
  public static final String LOG_SUBSCRIPTION_QUEUE_FULL_DROPPED_ARG_ENTRY_S_LAST_ARG_MS_2AD8AB3D = "Subscription queue full, dropped {} entry(s) in the last {} ms, latest ";
  public static final String LOG_SEARCHINDEX_ARG_QUEUESIZE_ARG_QUEUEREMAINING_ARG_2EA619ED = "searchIndex={}, queueSize={}, queueRemaining={}";
  public static final String LOG_SUBSCRIPTION_QUEUE_FULL_DROPPED_ENTRY_SEARCHINDEX_ARG_DROPPEDCOUNT_ARG_61F126B8 = "Subscription queue full, dropped entry searchIndex={}, droppedCount={}";
  public static final String LOG_RESERVED_ARG_BYTES_BATCH_ARG_ARG_CURRENT_TOTAL_USAGE_ARG_308AE9C2 = "Reserved {} bytes for batch {}-{}, current total usage {}";
  public static final String LOG_ARG_FAILED_SEND_IDLE_WRITER_SAFE_TIME_BARRIER_ARG_STATUS_AE047EAD = "{}: Failed to send idle writer safe-time barrier to {}. status={}";
  public static final String LOG_ARG_WRITE_OPERATION_FAILED_SEARCHINDEX_ARG_CODE_ARG_SUBSCRIPTIONQUEUES_ARG_THIS_ARG_F4B17576 = "{}: write operation failed. searchIndex: {}. Code: {}, subscriptionQueues: {}, this: {}";

}
