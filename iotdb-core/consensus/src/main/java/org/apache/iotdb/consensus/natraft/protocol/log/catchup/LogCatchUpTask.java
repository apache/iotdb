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

package org.apache.iotdb.consensus.natraft.protocol.log.catchup;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.exception.LeaderUnknownException;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.RaftRole;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.raft.thrift.AppendEntriesRequest;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/** LogCatchUpTask sends a list of logs to a node to make the node keep up with the leader. */
@SuppressWarnings("java:S2274") // enable timeout
public class LogCatchUpTask implements Callable<Boolean> {

  // sending logs may take longer than normal communications
  private long sendLogsWaitMs;
  private static final Logger logger = LoggerFactory.getLogger(LogCatchUpTask.class);
  Peer node;
  RaftMember raftMember;
  CatchUpManager catchUpManager;
  private List<Entry> logs;
  boolean abort = false;
  protected RaftConfig config;

  LogCatchUpTask(List<Entry> logs, Peer node, CatchUpManager catchUpManager, RaftConfig config) {
    this.logs = logs;
    this.node = node;
    this.raftMember = catchUpManager.getMember();
    this.catchUpManager = catchUpManager;
    this.sendLogsWaitMs = config.getWriteOperationTimeoutMS();
    this.config = config;
  }

  private AppendEntriesRequest prepareRequest(List<ByteBuffer> logList, int startPos) {
    AppendEntriesRequest request = new AppendEntriesRequest();

    request.setGroupId(raftMember.getRaftGroupId().convertToTConsensusGroupId());
    request.setLeader(raftMember.getThisNode().getEndpoint());
    request.setLeaderId(raftMember.getThisNode().getNodeId());
    request.setLeaderCommit(raftMember.getLogManager().getCommitLogIndex());

    // make sure this node is still a leader
    if (raftMember.getRole() != RaftRole.LEADER) {
      logger.debug("Leadership is lost when doing a catch-up to {}, aborting", node);
      abort = true;
      return null;
    }
    request.setTerm(raftMember.getStatus().getTerm().get());

    request.setEntries(logList);
    logger.debug("{}, node={} catchup request={}", raftMember.getName(), node, request);
    return request;
  }

  protected void doLogCatchUpInBatch() throws TException, InterruptedException {
    List<ByteBuffer> logList = new ArrayList<>();
    long totalLogSize = 0;
    int firstLogPos = 0;
    boolean batchFull;

    for (int i = 0; i < logs.size() && !abort; i++) {

      ByteBuffer logData = logs.get(i).serialize();
      int logSize = logData.array().length;
      if (logSize > config.getThriftMaxFrameSize() - IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
        logger.warn("the frame size {} of thrift is too small", config.getThriftMaxFrameSize());
        abort = true;
        return;
      }

      totalLogSize += logSize;
      // we should send logs who's size is smaller than the max frame size of thrift
      // left 200 byte for other fields of AppendEntriesRequest
      // send at most 100 logs a time to avoid long latency
      if (totalLogSize > config.getThriftMaxFrameSize() - IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
        // batch oversize, send previous batch and add the log to a new batch
        sendBatchLogs(logList, firstLogPos);
        logList.add(logData);
        firstLogPos = i;
        totalLogSize = logSize;
      } else {
        // just add the log the batch
        logList.add(logData);
      }

      batchFull = logList.size() >= config.getLogNumInBatch();
      if (batchFull) {
        sendBatchLogs(logList, firstLogPos);
        firstLogPos = i + 1;
        totalLogSize = 0;
      }
    }

    if (!logList.isEmpty()) {
      sendBatchLogs(logList, firstLogPos);
    }
  }

  private void sendBatchLogs(List<ByteBuffer> logList, int firstLogPos)
      throws TException, InterruptedException {
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} send logs from {} num {} for {}",
          raftMember.getThisNode(),
          logs.get(firstLogPos).getCurrLogIndex(),
          logList.size(),
          node);
    }
    AppendEntriesRequest request = prepareRequest(logList, firstLogPos);
    if (request == null) {
      return;
    }
    // do append entries
    if (logger.isInfoEnabled()) {
      logger.info("{}: sending {} logs to {}", raftMember.getName(), logList.size(), node);
    }
    abort = !appendEntriesAsync(logList, request);
    if (!abort && logger.isInfoEnabled()) {
      logger.info("{}: sent {} logs to {}", raftMember.getName(), logList.size(), node);
    }
    logList.clear();
  }

  private boolean appendEntriesAsync(List<ByteBuffer> logList, AppendEntriesRequest request)
      throws TException, InterruptedException {
    AtomicBoolean appendSucceed = new AtomicBoolean(false);

    LogCatchUpInBatchHandler handler = new LogCatchUpInBatchHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setRaftMember(raftMember);
    handler.setFollower(node);
    handler.setLogs(logList);
    synchronized (appendSucceed) {
      appendSucceed.set(false);
      AsyncRaftServiceClient client = raftMember.getClient(node.getEndpoint());
      if (client == null) {
        return false;
      }
      client.appendEntries(request, handler);
      catchUpManager.registerTask(node);
      appendSucceed.wait(sendLogsWaitMs);
    }
    return appendSucceed.get();
  }

  @Override
  public Boolean call() throws TException, InterruptedException, LeaderUnknownException {
    if (logs.isEmpty()) {
      return true;
    }

    doLogCatchUpInBatch();
    logger.info("{}: Catch up {} finished with result {}", raftMember.getName(), node, !abort);

    // the next catch up is enabled
    catchUpManager.unregisterTask(node);
    return !abort;
  }
}
