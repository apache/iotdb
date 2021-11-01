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

package org.apache.iotdb.cluster.expr;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogDispatcher.SendLogRequest;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.VotingLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.manage.MetaSingleSnapshotLogManager;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.DummyPlan;
import org.apache.iotdb.db.qp.physical.sys.LogPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequencerExpr extends MetaGroupMember {

  private static final Logger logger = LoggerFactory.getLogger(SequencerExpr.class);

  private int v2ThreadNum = 2000;
  private int v3ThreadNum = 0000;
  private AtomicLong reqCnt = new AtomicLong();

  private BlockingQueue<SendLogRequest> nonsequencedLogQueue = new ArrayBlockingQueue<>(
      4096);

  public SequencerExpr() {
    LogApplier applier = new LogApplier() {
      @Override
      public void apply(Log log) {
        log.setApplied(true);
      }

      @Override
      public void close() {

      }
    };
    logManager = new MetaSingleSnapshotLogManager(applier, this);

    new Thread(this::sequenceLog).start();
    new Thread(this::sequenceLog).start();
    new Thread(this::sequenceLog).start();
    new Thread(this::sequenceLog).start();
    reportThread = Executors.newSingleThreadScheduledExecutor();
    reportThread.scheduleAtFixedRate(
        this::generateNodeReport, REPORT_INTERVAL_SEC, REPORT_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  private TSStatus processPlanLocallyV2(PhysicalPlan plan) {
    logger.debug("{}: Processing plan {}", getName(), plan);
    // assign term and index to the new log and append it
    SendLogRequest sendLogRequest;

    Log log;
    if (plan instanceof LogPlan) {
      try {
        log = LogParser.getINSTANCE().parse(((LogPlan) plan).getLog());
      } catch (UnknownLogTypeException e) {
        logger.error("Can not parse LogPlan {}", plan, e);
        return StatusUtils.PARSE_LOG_ERROR;
      }
    } else {
      log = new PhysicalPlanLog();
      ((PhysicalPlanLog) log).setPlan(plan);
    }

    if (log.serialize().capacity() + Integer.BYTES
        >= ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize()) {
      logger.error(
          "Log cannot fit into buffer, please increase raft_log_buffer_size;"
              + "or reduce the size of requests you send.");
      return StatusUtils.INTERNAL_ERROR;
    }

    long startTime =
        Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2.getOperationStartTime();
    synchronized (logManager) {
      Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2.calOperationCostTimeFromStart(
          startTime);

      plan.setIndex(logManager.getLastLogIndex() + 1);
      log.setCurrLogTerm(getTerm().get());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      startTime = Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.getOperationStartTime();
      // just like processPlanLocally,we need to check the size of log

      // logDispatcher will serialize log, and set log size, and we will use the size after it
      logManager.append(log);
      Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.calOperationCostTimeFromStart(startTime);

      startTime = Statistic.RAFT_SENDER_BUILD_LOG_REQUEST.getOperationStartTime();
      sendLogRequest = buildSendLogRequest(log);
      Statistic.RAFT_SENDER_BUILD_LOG_REQUEST.calOperationCostTimeFromStart(startTime);

      startTime = Statistic.RAFT_SENDER_OFFER_LOG.getOperationStartTime();
      log.setCreateTime(System.nanoTime());
      votingLogList.insert(sendLogRequest.getVotingLog());
      getLogDispatcher().offer(sendLogRequest);
      Statistic.RAFT_SENDER_OFFER_LOG.calOperationCostTimeFromStart(startTime);
    }

    try {
      AppendLogResult appendLogResult =
          waitAppendResult(
              sendLogRequest.getVotingLog(),
              sendLogRequest.getLeaderShipStale(),
              sendLogRequest.getNewLeaderTerm(),
              sendLogRequest.getQuorumSize());
      Timer.Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_ACCEPT.calOperationCostTimeFromStart(
          sendLogRequest.getVotingLog().getLog().getCreateTime());

      switch (appendLogResult) {
        case WEAK_ACCEPT:
          // TODO: change to weak
          Statistic.RAFT_WEAK_ACCEPT.add(1);
          return StatusUtils.OK;
        case OK:
          logger.debug(MSG_LOG_IS_ACCEPTED, getName(), log);
          startTime = Timer.Statistic.RAFT_SENDER_COMMIT_LOG.getOperationStartTime();
          commitLog(log);
          Timer.Statistic.RAFT_SENDER_COMMIT_LOG.calOperationCostTimeFromStart(startTime);
          return StatusUtils.OK;
        case TIME_OUT:
          logger.debug("{}: log {} timed out...", getName(), log);
          break;
        case LEADERSHIP_STALE:
          // abort the appending, the new leader will fix the local logs by catch-up
        default:
          break;
      }
    } catch (Exception e) {
      return handleLogExecutionException(log, IOUtils.getRootCause(e));
    }
    return StatusUtils.TIME_OUT;
  }

  public SendLogRequest enqueueSendLogRequest(Log log) {
    VotingLog votingLog = buildVotingLog(log);
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(term.get());

    SendLogRequest sendLogRequest = new SendLogRequest(
        votingLog, leaderShipStale, newLeaderTerm, null, allNodes.size() / 2);
    try {
      nonsequencedLogQueue.put(sendLogRequest);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return sendLogRequest;
  }

  private void sequenceLog(List<SendLogRequest> sendLogRequests) {
    long startTime;
    synchronized (logManager) {
      for (SendLogRequest sendLogRequest : sendLogRequests) {
        Log log = sendLogRequest.getVotingLog().getLog();
        log.setCurrLogTerm(getTerm().get());
        log.setCurrLogIndex(logManager.getLastLogIndex() + 1);
        if (log instanceof PhysicalPlanLog) {
          ((PhysicalPlanLog) log).getPlan().setIndex(log.getCurrLogIndex());
        }

        startTime = Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.getOperationStartTime();
        // just like processPlanLocally,we need to check the size of log

        // logDispatcher will serialize log, and set log size, and we will use the size after it
        logManager.append(log);
        Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.calOperationCostTimeFromStart(startTime);

        AppendEntryRequest appendEntryRequest = buildAppendEntryRequest(log, false);
        sendLogRequest.setAppendEntryRequest(appendEntryRequest);

        startTime = Statistic.RAFT_SENDER_OFFER_LOG.getOperationStartTime();
        log.setCreateTime(System.nanoTime());
        votingLogList.insert(sendLogRequest.getVotingLog());
        getLogDispatcher().offer(sendLogRequest);
        Statistic.RAFT_SENDER_OFFER_LOG.calOperationCostTimeFromStart(startTime);
      }
    }
    sendLogRequests.clear();
  }

  private void sequenceLog() {
    List<SendLogRequest> sendLogRequests = new ArrayList<>();
    while (!Thread.interrupted()) {
      try {
        synchronized (nonsequencedLogQueue) {
          SendLogRequest request = nonsequencedLogQueue.take();
          sendLogRequests.add(request);
          nonsequencedLogQueue.drainTo(sendLogRequests);
        }

        sequenceLog(sendLogRequests);
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  private TSStatus processPlanLocallyV3(PhysicalPlan plan) {
    logger.debug("{}: Processing plan {}", getName(), plan);
    // assign term and index to the new log and append it
    SendLogRequest sendLogRequest;

    Log log;
    if (plan instanceof LogPlan) {
      try {
        log = LogParser.getINSTANCE().parse(((LogPlan) plan).getLog());
      } catch (UnknownLogTypeException e) {
        logger.error("Can not parse LogPlan {}", plan, e);
        return StatusUtils.PARSE_LOG_ERROR;
      }
    } else {
      log = new PhysicalPlanLog();
      ((PhysicalPlanLog) log).setPlan(plan);
    }

    if (log.serialize().capacity() + Integer.BYTES
        >= ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize()) {
      logger.error(
          "Log cannot fit into buffer, please increase raft_log_buffer_size;"
              + "or reduce the size of requests you send.");
      return StatusUtils.INTERNAL_ERROR;
    }

    long startTime;
    sendLogRequest = enqueueSendLogRequest(log);

    try {
      AppendLogResult appendLogResult =
          waitAppendResult(
              sendLogRequest.getVotingLog(),
              sendLogRequest.getLeaderShipStale(),
              sendLogRequest.getNewLeaderTerm(),
              sendLogRequest.getQuorumSize());
      Timer.Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_ACCEPT.calOperationCostTimeFromStart(
          sendLogRequest.getVotingLog().getLog().getCreateTime());

      switch (appendLogResult) {
        case WEAK_ACCEPT:
          // TODO: change to weak
          Statistic.RAFT_WEAK_ACCEPT.add(1);
          return StatusUtils.OK;
        case OK:
          logger.debug(MSG_LOG_IS_ACCEPTED, getName(), log);
          startTime = Timer.Statistic.RAFT_SENDER_COMMIT_LOG.getOperationStartTime();
          commitLog(log);
          Timer.Statistic.RAFT_SENDER_COMMIT_LOG.calOperationCostTimeFromStart(startTime);
          return StatusUtils.OK;
        case TIME_OUT:
          logger.debug("{}: log {} timed out...", getName(), log);
          break;
        case LEADERSHIP_STALE:
          // abort the appending, the new leader will fix the local logs by catch-up
        default:
          break;
      }
    } catch (Exception e) {
      return handleLogExecutionException(log, IOUtils.getRootCause(e));
    }
    return StatusUtils.TIME_OUT;
  }

  @Override
  public Client getSyncClient(Node node) {
    return new Client(null, null) {
      @Override
      public AppendEntryResult appendEntry(AppendEntryRequest request) {
        return new AppendEntryResult().setStatus(Response.RESPONSE_STRONG_ACCEPT);
      }

      @Override
      public AppendEntryResult appendEntries(AppendEntriesRequest request) {
        return new AppendEntryResult().setStatus(Response.RESPONSE_STRONG_ACCEPT);
      }
    };
  }

  private void decentralizedSequencing() {
    for (int i = 0; i < v2ThreadNum; i++) {
      new Thread(() -> {
        while (true) {
          reqCnt.incrementAndGet();
          DummyPlan dummyPlan = new DummyPlan();
          processPlanLocallyV2(dummyPlan);
        }
      }).start();
    }
  }

  private void centralizedSequencing() {
    for (int i = 0; i < v3ThreadNum; i++) {
      new Thread(() -> {
        while (true) {
          reqCnt.incrementAndGet();
          DummyPlan dummyPlan = new DummyPlan();
          processPlanLocallyV3(dummyPlan);
        }
      }).start();
    }
  }

  private void startMonitor() {
    new Thread(() -> {
      long startTime = System.currentTimeMillis();
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        long consumedTime = System.currentTimeMillis() - startTime;
        System.out.println("" + consumedTime + ", " + (reqCnt.get() * 1.0 / consumedTime * 1000L));
      }
    }).start();
  }

  public static void main(String[] args) {
    RaftMember.USE_LOG_DISPATCHER = true;
    ClusterDescriptor.getInstance().getConfig().setEnableRaftLogPersistence(false);
    SequencerExpr sequencerExpr = new SequencerExpr();
    sequencerExpr.setCharacter(NodeCharacter.LEADER);
    PartitionGroup group = new PartitionGroup();
    for (int i = 0; i < 3; i++) {
      group.add(new Node().setNodeIdentifier(i).setMetaPort(i));
    }
    sequencerExpr.setAllNodes(group);
    sequencerExpr.centralizedSequencing();
    sequencerExpr.decentralizedSequencing();
    sequencerExpr.startMonitor();
  }
}
