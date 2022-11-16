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
package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.VotingLog;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_LOG_MISMATCH;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_OUT_OF_WINDOW;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_STRONG_ACCEPT;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_WEAK_ACCEPT;

/**
 * AppendNodeEntryHandler checks if the log is successfully appended by the quorum or some node has
 * rejected it for some reason when one node has finished the AppendEntryRequest. The target of the
 * log is the single nodes, it requires the agreement from the quorum of the nodes to reach
 * consistency.
 */
public class AppendNodeEntryHandler implements AsyncMethodCallback<AppendEntryResult> {

  private static final Logger logger = LoggerFactory.getLogger(AppendNodeEntryHandler.class);

  protected RaftMember member;
  protected VotingLog log;
  protected Node directReceiver;
  protected int quorumSize;

  // nano start time when the send begins
  private long sendStart = Long.MIN_VALUE;

  public AppendNodeEntryHandler() {
    if (Timer.ENABLE_INSTRUMENTING
        && ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      sendStart = System.nanoTime();
    }
  }

  // TODO-remove
  private static Map<Long, Integer> entryAcceptedTimes = new ConcurrentHashMap<>();

  @Override
  public void onComplete(AppendEntryResult response) {
    if (Timer.ENABLE_INSTRUMENTING) {
      Statistic.RAFT_SENDER_SEND_LOG_ASYNC.calOperationCostTimeFromStart(sendStart);
    }
    if (log.isHasFailed()) {
      return;
    }

    Node trueReceiver = response.isSetReceiver() ? response.receiver : directReceiver;

    logger.debug(
        "{}: Append response {} from {} for log {}", member.getName(), response, trueReceiver, log);

    long resp = response.status;

    if (resp == RESPONSE_STRONG_ACCEPT || resp == RESPONSE_AGREE) {
      Integer count =
          entryAcceptedTimes.compute(
              log.getLog().getCurrLogIndex(),
              (index, cnt) -> {
                if (cnt == null) {
                  cnt = 1;
                } else {
                  cnt = cnt + 1;
                }
                return cnt;
              });
      if (count == quorumSize) {
        Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_ACCEPT.calOperationCostTimeFromStart(
            log.getLog().getCreateTime());
      }
      member
          .getVotingLogList()
          .onStronglyAccept(
              log.getLog().getCurrLogIndex(),
              log.getLog().getCurrLogTerm(),
              trueReceiver,
              response.signature);

      member.getPeer(trueReceiver).setMatchIndex(response.lastLogIndex);
    } else if (resp > 0) {
      // a response > 0 is the follower's term
      // the leadership is stale, wait for the new leader's heartbeat
      logger.debug(
          "{}: Received a rejection from {} because term is stale: {}, log: {}",
          member.getName(),
          trueReceiver,
          resp,
          log);
      member.stepDown(resp, false);
      synchronized (log) {
        log.notifyAll();
      }
      if (ClusterDescriptor.getInstance().getConfig().isUseIndirectBroadcasting()) {
        member.removeAppendLogHandler(
            new Pair<>(log.getLog().getCurrLogIndex(), log.getLog().getCurrLogTerm()));
      }
    } else if (resp == RESPONSE_WEAK_ACCEPT) {
      synchronized (log) {
        log.getWeaklyAcceptedNodeIds().add(trueReceiver.nodeIdentifier);
        log.notifyAll();
      }
    } else {
      // e.g., Response.RESPONSE_LOG_MISMATCH
      if (resp == RESPONSE_LOG_MISMATCH || resp == RESPONSE_OUT_OF_WINDOW) {
        logger.debug(
            "{}: The log {} is rejected by {} because: {}",
            member.getName(),
            log,
            trueReceiver,
            resp);
      } else {
        logger.warn(
            "{}: The log {} is rejected by {} because: {}",
            member.getName(),
            log,
            trueReceiver,
            resp);
        onFail(trueReceiver);
      }
    }
    // rejected because the receiver's logs are stale or the receiver has no cluster info, just
    // wait for the heartbeat to handle
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof ConnectException) {
      logger.warn(
          "{}: Cannot append log {}: cannot connect to {}: {}",
          member.getName(),
          log,
          directReceiver,
          exception.getMessage());
    } else {
      logger.warn(
          "{}: Cannot append log {} to {}", member.getName(), log, directReceiver, exception);
    }
    onFail(directReceiver);
  }

  private void onFail(Node trueReceiver) {
    synchronized (log) {
      log.getFailedNodeIds().add(trueReceiver.nodeIdentifier);
      if (log.getFailedNodeIds().size() > quorumSize) {
        // quorum members have failed, there is no need to wait for others
        log.setHasFailed(true);
        log.notifyAll();
        if (ClusterDescriptor.getInstance().getConfig().isUseIndirectBroadcasting()) {
          member.removeAppendLogHandler(
              new Pair<>(log.getLog().getCurrLogIndex(), log.getLog().getCurrLogTerm()));
        }
      }
    }
  }

  public void setLog(VotingLog log) {
    this.log = log;
  }

  public void setMember(RaftMember member) {
    this.member = member;
  }

  public void setDirectReceiver(Node follower) {
    this.directReceiver = follower;
  }

  public void setQuorumSize(int quorumSize) {
    this.quorumSize = quorumSize;
  }
}
