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

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogDispatcher.SendLogRequest;
import org.apache.iotdb.cluster.log.VotingLog;
import org.apache.iotdb.cluster.log.logtypes.RequestLog;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.LogPlan;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SynchronizedSequencer performs sequencing by taking the monitor of a LogManager within the caller
 * thread.
 */
public class SynchronousSequencer implements LogSequencer {

  private RaftMember member;
  private RaftLogManager logManager;

  public SynchronousSequencer(RaftMember member, RaftLogManager logManager) {
    this.member = member;
    this.logManager = logManager;
  }

  private SendLogRequest enqueueEntry(SendLogRequest sendLogRequest) {
    long startTime = Statistic.RAFT_SENDER_OFFER_LOG.getOperationStartTime();

    if (member.getAllNodes().size() > 1) {
      member.getLogDispatcher().offer(sendLogRequest);
    }
    Statistic.RAFT_SENDER_OFFER_LOG.calOperationCostTimeFromStart(startTime);
    return sendLogRequest;
  }

  @Override
  public SendLogRequest sequence(Log log) {
    SendLogRequest sendLogRequest = null;

    long startTime =
        Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2.getOperationStartTime();
    long startWaitingTime = System.currentTimeMillis();

    while (true) {
      synchronized (logManager) {
        if (!IoTDBDescriptor.getInstance().getConfig().isEnableMemControl()
            || (logManager.getLastLogIndex() - logManager.getCommitLogIndex()
                <= ClusterDescriptor.getInstance()
                    .getConfig()
                    .getUnCommittedRaftLogNumForRejectThreshold())) {
          Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2.calOperationCostTimeFromStart(
              startTime);

          // if the log contains a physical plan which is not a LogPlan, assign the same index to
          // the plan so the state machine can be bridged with the consensus
          if (log instanceof RequestLog
              && (((RequestLog) log).getRequest() instanceof PhysicalPlan)
              && !(((RequestLog) log).getRequest() instanceof LogPlan)) {
            ((PhysicalPlan) ((RequestLog) log).getRequest())
                .setIndex(logManager.getLastLogIndex() + 1);
          }
          log.setCurrLogTerm(member.getTerm().get());
          log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

          startTime = Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.getOperationStartTime();
          // logDispatcher will serialize log, and set log size, and we will use the size after it
          logManager.append(log);
          Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.calOperationCostTimeFromStart(startTime);

          startTime = Statistic.RAFT_SENDER_BUILD_LOG_REQUEST.getOperationStartTime();
          sendLogRequest = buildSendLogRequest(log);
          log.setCreateTime(System.nanoTime());
          Statistic.RAFT_SENDER_BUILD_LOG_REQUEST.calOperationCostTimeFromStart(startTime);

          if (!(ClusterDescriptor.getInstance().getConfig().isUseFollowerSlidingWindow()
              && ClusterDescriptor.getInstance().getConfig().isEnableWeakAcceptance())) {
            sendLogRequest = enqueueEntry(sendLogRequest);
          }
          break;
        }
        try {
          TimeUnit.MILLISECONDS.sleep(
              IoTDBDescriptor.getInstance().getConfig().getCheckPeriodWhenInsertBlocked());
          if (System.currentTimeMillis() - startWaitingTime
              > IoTDBDescriptor.getInstance().getConfig().getMaxWaitingTimeWhenInsertBlocked()) {
            return null;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (ClusterDescriptor.getInstance().getConfig().isUseFollowerSlidingWindow()
        && ClusterDescriptor.getInstance().getConfig().isEnableWeakAcceptance()) {
      sendLogRequest = enqueueEntry(sendLogRequest);
    }

    return sendLogRequest;
  }

  @Override
  public void setLogManager(RaftLogManager logManager) {
    this.logManager = logManager;
  }

  private SendLogRequest buildSendLogRequest(Log log) {
    VotingLog votingLog = member.buildVotingLog(log);
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(member.getTerm().get());

    long startTime = Statistic.RAFT_SENDER_BUILD_APPEND_REQUEST.getOperationStartTime();
    AppendEntryRequest appendEntryRequest = member.buildAppendEntryRequest(log, false);
    Statistic.RAFT_SENDER_BUILD_APPEND_REQUEST.calOperationCostTimeFromStart(startTime);

    return new SendLogRequest(
        votingLog,
        leaderShipStale,
        newLeaderTerm,
        appendEntryRequest,
        member.getAllNodes().size() / 2);
  }

  public static class Factory implements LogSequencerFactory {

    @Override
    public LogSequencer create(RaftMember member, RaftLogManager logManager) {
      return new SynchronousSequencer(member, logManager);
    }
  }

  @Override
  public void close() {}
}
