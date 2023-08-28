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

package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.ratis.metrics.RatisMetricsManager;
import org.apache.iotdb.consensus.ratis.utils.Utils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ApplicationStateMachineProxy extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(ApplicationStateMachineProxy.class);
  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();
  private final IStateMachine applicationStateMachine;
  private final IStateMachine.RetryPolicy retryPolicy;
  private final SnapshotStorage snapshotStorage;
  private final RaftGroupId groupId;
  private final ConsensusGroupId consensusGroupId;
  private final TConsensusGroupType consensusGroupType;
  private final ConcurrentHashMap<ConsensusGroupId, AtomicBoolean> canStaleRead;

  ApplicationStateMachineProxy(IStateMachine stateMachine, RaftGroupId id) {
    this(stateMachine, id, null);
  }

  ApplicationStateMachineProxy(
      IStateMachine stateMachine,
      RaftGroupId id,
      ConcurrentHashMap<ConsensusGroupId, AtomicBoolean> canStaleRead) {
    this.applicationStateMachine = stateMachine;
    this.canStaleRead = canStaleRead;
    this.groupId = id;
    this.consensusGroupId = Utils.fromRaftGroupIdToConsensusGroupId(id);
    retryPolicy =
        applicationStateMachine instanceof IStateMachine.RetryPolicy
            ? (IStateMachine.RetryPolicy) applicationStateMachine
            : new IStateMachine.RetryPolicy() {};
    snapshotStorage = new SnapshotStorage(applicationStateMachine, groupId);
    consensusGroupType = Utils.getConsensusGroupTypeFromPrefix(groupId.toString());
    applicationStateMachine.start();
  }

  @Override
  public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage)
      throws IOException {
    getLifeCycle()
        .startAndTransition(
            () -> {
              snapshotStorage.init(storage);
              loadSnapshot(snapshotStorage.findLatestSnapshotDir());
            });
  }

  @Override
  public void reinitialize() {
    setLastAppliedTermIndex(null);
    loadSnapshot(snapshotStorage.findLatestSnapshotDir());
    if (getLifeCycleState() == LifeCycle.State.PAUSED) {
      getLifeCycle().transition(LifeCycle.State.STARTING);
      getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
  }

  @Override
  public void pause() {
    if (getLifeCycleState() == LifeCycle.State.RUNNING) {
      getLifeCycle().transition(LifeCycle.State.PAUSING);
      getLifeCycle().transition(LifeCycle.State.PAUSED);
    }
  }

  @Override
  public void close() throws IOException {
    getLifeCycle().checkStateAndClose(applicationStateMachine::stop);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    boolean isLeader = false;
    long writeToStateMachineStartTime = System.nanoTime();
    RaftProtos.LogEntryProto log = trx.getLogEntry();
    updateLastAppliedTermIndex(log.getTerm(), log.getIndex());

    IConsensusRequest applicationRequest;

    // if this server is leader
    // it will first try to obtain applicationRequest from transaction context
    if (trx.getClientRequest() != null
        && trx.getClientRequest().getMessage() instanceof RequestMessage) {
      RequestMessage requestMessage = (RequestMessage) trx.getClientRequest().getMessage();
      applicationRequest = requestMessage.getActualRequest();
      isLeader = true;
    } else {
      applicationRequest =
          new ByteBufferConsensusRequest(
              log.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    }

    Message ret = null;
    waitUntilSystemAllowApply();
    TSStatus finalStatus = null;
    boolean shouldRetry = false;
    boolean firstTry = true;
    do {
      try {
        if (!firstTry) {
          Thread.sleep(retryPolicy.getSleepTime());
        }
        IConsensusRequest deserializedRequest =
            applicationStateMachine.deserializeRequest(applicationRequest);

        TSStatus result = applicationStateMachine.write(deserializedRequest);

        if (firstTry) {
          finalStatus = result;
          firstTry = false;
        } else {
          finalStatus = retryPolicy.updateResult(finalStatus, result);
        }

        shouldRetry = retryPolicy.shouldRetry(finalStatus);
        if (!shouldRetry) {
          ret = new ResponseMessage(finalStatus);
          break;
        }
      } catch (InterruptedException i) {
        logger.warn("{} interrupted when retry sleep", this);
        Thread.currentThread().interrupt();
      } catch (Throwable rte) {
        logger.error("application statemachine throws a runtime exception: ", rte);
        ret =
            new ResponseMessage(
                new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                    .setMessage("internal error. statemachine throws a runtime exception: " + rte));
        if (Utils.stallApply()) {
          waitUntilSystemAllowApply();
          shouldRetry = true;
        } else {
          break;
        }
      }
    } while (shouldRetry);
    if (isLeader) {
      // only record time cost for data region in Performance Overview Dashboard
      if (consensusGroupType == TConsensusGroupType.DataRegion) {
        PERFORMANCE_OVERVIEW_METRICS.recordEngineCost(
            System.nanoTime() - writeToStateMachineStartTime);
      }
      // statistic the time of write stateMachine
      RatisMetricsManager.getInstance()
          .recordWriteStateMachineCost(
              System.nanoTime() - writeToStateMachineStartTime, consensusGroupType);
    }
    return CompletableFuture.completedFuture(ret);
  }

  private void waitUntilSystemAllowApply() {
    while (Utils.stallApply()) {
      try {
        TimeUnit.SECONDS.sleep(60);
      } catch (InterruptedException e) {
        logger.warn("{}: interrupted when waiting until system ready: ", this, e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    if (!(request instanceof RequestMessage)) {
      // return null dataset to indicate an error
      logger.error("An RequestMessage is required but got {}", request);
      return CompletableFuture.completedFuture(new ResponseMessage(null));
    }
    RequestMessage requestMessage = (RequestMessage) request;
    DataSet result = applicationStateMachine.read(requestMessage.getActualRequest());
    return CompletableFuture.completedFuture(new ResponseMessage(result));
  }

  @Override
  public long takeSnapshot() throws IOException {
    final TermIndex lastApplied = getLastAppliedTermIndex();
    if (lastApplied.getTerm() <= 0 || lastApplied.getIndex() <= 0) {
      return RaftLog.INVALID_LOG_INDEX;
    }

    // require the application statemachine to take the latest snapshot
    final String metadata = Utils.getMetadataFromTermIndex(lastApplied);
    File snapshotTmpDir = snapshotStorage.getSnapshotTmpDir(metadata);

    // delete snapshotDir fully in case of last takeSnapshot() crashed
    FileUtils.deleteFully(snapshotTmpDir);

    snapshotTmpDir.mkdirs();
    if (!snapshotTmpDir.isDirectory()) {
      logger.error("Unable to create temp snapshotDir at {}", snapshotTmpDir);
      return RaftLog.INVALID_LOG_INDEX;
    }

    boolean applicationTakeSnapshotSuccess =
        applicationStateMachine.takeSnapshot(
            snapshotTmpDir, snapshotStorage.getSnapshotTmpId(metadata), metadata);
    if (!applicationTakeSnapshotSuccess) {
      deleteIncompleteSnapshot(snapshotTmpDir);
      return RaftLog.INVALID_LOG_INDEX;
    }

    File snapshotDir = snapshotStorage.getSnapshotDir(metadata);

    FileUtils.deleteFully(snapshotDir);
    try {
      Files.move(snapshotTmpDir.toPath(), snapshotDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      logger.error(
          "{} atomic rename {} to {} failed with exception {}",
          this,
          snapshotTmpDir,
          snapshotDir,
          e);
      deleteIncompleteSnapshot(snapshotTmpDir);
      return RaftLog.INVALID_LOG_INDEX;
    }

    snapshotStorage.updateSnapshotCache();

    return lastApplied.getIndex();
  }

  private void deleteIncompleteSnapshot(File snapshotDir) throws IOException {
    // this takeSnapshot failed, clean up files and directories
    // statemachine is supposed to clear snapshotDir on failure
    boolean isEmpty = snapshotDir.delete();
    if (!isEmpty) {
      logger.info("Snapshot directory is incomplete, deleting {}", snapshotDir.getAbsolutePath());
      FileUtils.deleteFully(snapshotDir);
    }
  }

  private void loadSnapshot(File latestSnapshotDir) {
    snapshotStorage.updateSnapshotCache();
    if (latestSnapshotDir == null) {
      return;
    }

    // require the application statemachine to load the latest snapshot
    applicationStateMachine.loadSnapshot(latestSnapshotDir);
    TermIndex snapshotTermIndex = Utils.getTermIndexFromDir(latestSnapshotDir);
    updateLastAppliedTermIndex(snapshotTermIndex.getTerm(), snapshotTermIndex.getIndex());
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return snapshotStorage;
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
    Optional.ofNullable(canStaleRead)
        .ifPresent(
            m -> m.computeIfAbsent(consensusGroupId, id -> new AtomicBoolean(false)).set(false));
    applicationStateMachine
        .event()
        .notifyLeaderChanged(
            Utils.fromRaftGroupIdToConsensusGroupId(groupMemberId.getGroupId()),
            Utils.fromRaftPeerIdToNodeId(newLeaderId));
  }

  @Override
  public void notifyConfigurationChanged(
      long term, long index, RaftConfigurationProto newRaftConfiguration) {
    applicationStateMachine
        .event()
        .notifyConfigurationChanged(
            term,
            index,
            Utils.fromRaftProtoListAndRaftGroupIdToPeers(
                newRaftConfiguration.getPeersList(), groupId));
  }
}
