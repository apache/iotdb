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

package org.apache.iotdb.cluster.log.sequencing;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogDispatcher.SendLogRequest;
import org.apache.iotdb.cluster.log.VotingLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.cluster.server.monitor.Timer.Statistic.RAFT_SENDER_SEQUENCE_LOG;

public class AsynchronousSequencer implements LogSequencer {

  private static final Logger logger = LoggerFactory.getLogger(AsynchronousSequencer.class);
  private static final ExecutorService SEQUENCER_POOL =
      IoTDBThreadPoolFactory.newCachedThreadPool("SequencerPool");
  private static final int SEQUENCER_PARALLELISM = 4;

  private RaftMember member;
  private RaftLogManager logManager;

  private BlockingQueue<SendLogRequest> unsequencedLogQueue;

  public AsynchronousSequencer(RaftMember member, RaftLogManager logManager) {
    this.member = member;
    this.logManager = logManager;
    unsequencedLogQueue = new ArrayBlockingQueue<>(40960, true);
    for (int i = 0; i < SEQUENCER_PARALLELISM; i++) {
      SEQUENCER_POOL.submit(this::sequenceTask);
    }
  }

  public SendLogRequest enqueueSendLogRequest(Log log) {
    VotingLog votingLog = member.buildVotingLog(log);
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(member.getTerm().get());

    SendLogRequest request =
        new SendLogRequest(
            votingLog, leaderShipStale, newLeaderTerm, null, member.getAllNodes().size() / 2);
    try {
      unsequencedLogQueue.put(request);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Interrupted while putting {}", log);
    }
    return request;
  }

  private void sequenceLogs(List<SendLogRequest> sendLogRequests) {
    long startTime;
    synchronized (logManager) {
      for (SendLogRequest sendLogRequest : sendLogRequests) {
        long sequenceStartTime = RAFT_SENDER_SEQUENCE_LOG.getOperationStartTime();
        Log log = sendLogRequest.getVotingLog().getLog();
        log.setSequenceStartTime(sequenceStartTime);
        log.setCurrLogTerm(member.getTerm().get());
        log.setCurrLogIndex(logManager.getLastLogIndex() + 1);
        if (log instanceof PhysicalPlanLog) {
          ((PhysicalPlanLog) log).getPlan().setIndex(log.getCurrLogIndex());
        }

        startTime = Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.getOperationStartTime();
        // just like processPlanLocally,we need to check the size of log

        // logDispatcher will serialize log, and set log size, and we will use the size after it
        logManager.append(log);
        Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.calOperationCostTimeFromStart(startTime);

        AppendEntryRequest appendEntryRequest = member.buildAppendEntryRequest(log, false);
        sendLogRequest.setAppendEntryRequest(appendEntryRequest);

        startTime = Statistic.RAFT_SENDER_OFFER_LOG.getOperationStartTime();
        Statistic.LOG_DISPATCHER_FROM_RECEIVE_TO_CREATE.calOperationCostTimeFromStart(
            log.getReceiveTime());
        log.setCreateTime(System.nanoTime());
        if (member.getAllNodes().size() > 1) {
          member.getVotingLogList().insert(sendLogRequest.getVotingLog());
          member.getLogDispatcher().offer(sendLogRequest);
        }
        Statistic.RAFT_SENDER_OFFER_LOG.calOperationCostTimeFromStart(startTime);
        RAFT_SENDER_SEQUENCE_LOG.calOperationCostTimeFromStart(sequenceStartTime);
      }
    }
    sendLogRequests.clear();
  }

  private void sequenceTask() {
    List<SendLogRequest> sendLogRequests = new ArrayList<>();
    while (!Thread.interrupted()) {
      try {
        synchronized (unsequencedLogQueue) {
          SendLogRequest request = unsequencedLogQueue.take();
          sendLogRequests.add(request);
          unsequencedLogQueue.drainTo(sendLogRequests);
        }

        sequenceLogs(sendLogRequests);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        logger.error("Error in sequencer", e);
        return;
      }
    }
  }

  @Override
  public SendLogRequest sequence(Log log) {
    return enqueueSendLogRequest(log);
  }

  @Override
  public void setLogManager(RaftLogManager logManager) {
    this.logManager = logManager;
  }

  public static class Factory implements LogSequencerFactory {

    @Override
    public LogSequencer create(RaftMember member, RaftLogManager logManager) {
      return new AsynchronousSequencer(member, logManager);
    }
  }
}
