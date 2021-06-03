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

package org.apache.iotdb.cluster.log.catchup;

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.LogCatchUpHandler;
import org.apache.iotdb.cluster.server.handlers.caller.LogCatchUpInBatchHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.TestOnly;

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
  private static final long SEND_LOGS_WAIT_MS = RaftServer.getWriteOperationTimeoutMS();
  private static final Logger logger = LoggerFactory.getLogger(LogCatchUpTask.class);
  Node node;
  RaftMember raftMember;
  private List<Log> logs;
  private boolean useBatch = ClusterDescriptor.getInstance().getConfig().isUseBatchInLogCatchUp();
  boolean abort = false;
  private int raftId;

  LogCatchUpTask(List<Log> logs, Node node, int raftId, RaftMember raftMember) {
    this.logs = logs;
    this.node = node;
    this.raftId = raftId;
    this.raftMember = raftMember;
  }

  @TestOnly
  LogCatchUpTask(List<Log> logs, Node node, int raftId, RaftMember raftMember, boolean useBatch) {
    this.logs = logs;
    this.node = node;
    this.raftId = raftId;
    this.raftMember = raftMember;
    this.useBatch = useBatch;
  }

  @TestOnly
  void setUseBatch(boolean useBatch) {
    this.useBatch = useBatch;
  }

  void doLogCatchUp() throws TException, InterruptedException, LeaderUnknownException {
    AppendEntryRequest request = new AppendEntryRequest();
    if (raftMember.getHeader() != null) {
      request.setHeader(raftMember.getHeader());
    }
    request.setLeader(raftMember.getThisNode());
    request.setLeaderCommit(raftMember.getLogManager().getCommitLogIndex());

    for (int i = 0; i < logs.size() && !abort; i++) {
      Log log = logs.get(i);
      synchronized (raftMember.getTerm()) {
        // make sure this node is still a leader
        if (raftMember.getCharacter() != NodeCharacter.LEADER) {
          throw new LeaderUnknownException(raftMember.getAllNodes());
        }
        request.setTerm(raftMember.getTerm().get());
      }
      request.setPrevLogIndex(log.getCurrLogIndex() - 1);
      if (i == 0) {
        try {
          request.setPrevLogTerm(raftMember.getLogManager().getTerm(log.getCurrLogIndex() - 1));
        } catch (Exception e) {
          logger.error("getTerm failed for newly append entries", e);
        }
      } else {
        request.setPrevLogTerm(logs.get(i - 1).getCurrLogTerm());
      }

      if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
        abort = !appendEntryAsync(log, request);
      } else {
        abort = !appendEntrySync(log, request);
      }
    }
  }

  private boolean appendEntryAsync(Log log, AppendEntryRequest request)
      throws TException, InterruptedException {
    LogCatchUpHandler handler = getCatchUpHandler(log, request);
    synchronized (handler.getAppendSucceed()) {
      AsyncClient client = raftMember.getAsyncClient(node);
      if (client == null) {
        return false;
      }
      client.appendEntry(request, handler);
      raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
      handler.getAppendSucceed().wait(RaftServer.getWriteOperationTimeoutMS());
    }
    return handler.getAppendSucceed().get();
  }

  private LogCatchUpHandler getCatchUpHandler(Log log, AppendEntryRequest request) {
    AtomicBoolean appendSucceed = new AtomicBoolean(false);
    LogCatchUpHandler handler = new LogCatchUpHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setRaftMember(raftMember);
    handler.setFollower(node);
    handler.setLog(log);
    request.setEntry(log.serialize());
    return handler;
  }

  private boolean appendEntrySync(Log log, AppendEntryRequest request) {
    LogCatchUpHandler handler = getCatchUpHandler(log, request);

    Client client = raftMember.getSyncClient(node);
    if (client == null) {
      logger.error("No available client for {} when append entry", node);
      return false;
    }

    try {
      long result = client.appendEntry(request);
      handler.onComplete(result);
      return handler.getAppendSucceed().get();
    } catch (TException e) {
      client.getInputProtocol().getTransport().close();
      handler.onError(e);
      return false;
    } finally {
      ClientUtils.putBackSyncClient(client);
    }
  }

  private AppendEntriesRequest prepareRequest(List<ByteBuffer> logList, int startPos) {
    AppendEntriesRequest request = new AppendEntriesRequest();

    if (raftMember.getHeader() != null) {
      request.setHeader(raftMember.getHeader());
    }
    request.setLeader(raftMember.getThisNode());
    request.setLeaderCommit(raftMember.getLogManager().getCommitLogIndex());

    synchronized (raftMember.getTerm()) {
      // make sure this node is still a leader
      if (raftMember.getCharacter() != NodeCharacter.LEADER) {
        logger.debug("Leadership is lost when doing a catch-up to {}, aborting", node);
        abort = true;
        return null;
      }
      request.setTerm(raftMember.getTerm().get());
    }

    request.setEntries(logList);
    // set index for raft
    request.setPrevLogIndex(logs.get(startPos).getCurrLogIndex() - 1);
    if (startPos != 0) {
      request.setPrevLogTerm(logs.get(startPos - 1).getCurrLogTerm());
    } else {
      try {
        request.setPrevLogTerm(
            raftMember.getLogManager().getTerm(logs.get(0).getCurrLogIndex() - 1));
      } catch (Exception e) {
        logger.error("getTerm failed for newly append entries", e);
      }
    }
    logger.debug("{}, node={} catchup request={}", raftMember.getName(), node, request);
    return request;
  }

  private void doLogCatchUpInBatch() throws TException, InterruptedException {
    List<ByteBuffer> logList = new ArrayList<>();
    long totalLogSize = 0;
    int firstLogPos = 0;
    boolean batchFull;

    for (int i = 0; i < logs.size() && !abort; i++) {

      ByteBuffer logData = logs.get(i).serialize();
      int logSize = logData.array().length;
      if (logSize
          > IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize()
              - IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
        logger.warn(
            "the frame size {} of thrift is too small",
            IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize());
        abort = true;
        return;
      }

      totalLogSize += logSize;
      // we should send logs who's size is smaller than the max frame size of thrift
      // left 200 byte for other fields of AppendEntriesRequest
      // send at most 100 logs a time to avoid long latency
      if (totalLogSize
          > IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize()
              - IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
        // batch oversize, send previous batch and add the log to a new batch
        sendBatchLogs(logList, firstLogPos);
        logList.add(logData);
        firstLogPos = i;
        totalLogSize = logSize;
      } else {
        // just add the log the batch
        logList.add(logData);
      }

      batchFull = logList.size() >= ClusterConstant.LOG_NUM_IN_BATCH;
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
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      abort = !appendEntriesAsync(logList, request);
    } else {
      abort = !appendEntriesSync(logList, request);
    }
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
      AsyncClient client = raftMember.getAsyncClient(node);
      if (client == null) {
        return false;
      }
      client.appendEntries(request, handler);
      raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
      appendSucceed.wait(SEND_LOGS_WAIT_MS);
    }
    return appendSucceed.get();
  }

  private boolean appendEntriesSync(List<ByteBuffer> logList, AppendEntriesRequest request) {
    AtomicBoolean appendSucceed = new AtomicBoolean(false);
    LogCatchUpInBatchHandler handler = new LogCatchUpInBatchHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setRaftMember(raftMember);
    handler.setFollower(node);
    handler.setLogs(logList);

    Client client = raftMember.getSyncClient(node);
    if (client == null) {
      logger.error("No available client for {} when append entries", node);
      return false;
    }
    try {
      long result = client.appendEntries(request);
      handler.onComplete(result);
      return appendSucceed.get();
    } catch (TException e) {
      client.getInputProtocol().getTransport().close();
      handler.onError(e);
      logger.warn("Failed logs: {}, first index: {}", logList, request.prevLogIndex + 1);
      return false;
    } finally {
      ClientUtils.putBackSyncClient(client);
    }
  }

  @Override
  public Boolean call() throws TException, InterruptedException, LeaderUnknownException {
    if (logs.isEmpty()) {
      return true;
    }

    if (useBatch) {
      doLogCatchUpInBatch();
    } else {
      doLogCatchUp();
    }
    logger.info("{}: Catch up {} finished with result {}", raftMember.getName(), node, !abort);

    // the next catch up is enabled
    raftMember.getLastCatchUpResponseTime().remove(node);
    return !abort;
  }
}
