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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.LogCatchUpHandler;
import org.apache.iotdb.cluster.server.handlers.caller.LogCatchUpInBatchHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogCatchUpTask sends a list of logs to a node to make the node keep up with the leader.
 */
public class LogCatchUpTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(LogCatchUpTask.class);
  private static final int LOG_NUM_IN_BATCH = 128;
  Node node;
  RaftMember raftMember;
  private List<Log> logs;
  private boolean useBatch = ClusterDescriptor.getInstance().getConfig().isUseBatchInLogCatchUp();

  public LogCatchUpTask(List<Log> logs, Node node, RaftMember raftMember) {
    this.logs = logs;
    this.node = node;
    this.raftMember = raftMember;
  }

  @TestOnly
  public LogCatchUpTask(List<Log> logs, Node node, RaftMember raftMember, boolean useBatch) {
    this.logs = logs;
    this.node = node;
    this.raftMember = raftMember;
    this.useBatch = useBatch;
  }

  @TestOnly
  public void setUseBatch(boolean useBatch) {
    this.useBatch = useBatch;
  }

  void doLogCatchUp() throws TException, InterruptedException {

    AppendEntryRequest request = new AppendEntryRequest();
    AtomicBoolean appendSucceed = new AtomicBoolean(false);
    boolean abort = false;

    LogCatchUpHandler handler = new LogCatchUpHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setRaftMember(raftMember);
    handler.setFollower(node);
    if (raftMember.getHeader() != null) {
      request.setHeader(raftMember.getHeader());
    }
    request.setLeader(raftMember.getThisNode());
    long commitIndex;
    synchronized (raftMember.getLogManager()) {
      commitIndex = raftMember.getLogManager().getCommitLogIndex();
    }
    request.setLeaderCommit(commitIndex);

    for (int i = 0; i < logs.size() && !abort; i++) {
      Log log = logs.get(i);
      synchronized (raftMember.getTerm()) {
        // make sure this node is still a leader
        if (raftMember.getCharacter() != NodeCharacter.LEADER) {
          logger.debug("Leadership is lost when doing a catch-up to {}, aborting", node);
          break;
        }
        request.setTerm(raftMember.getTerm().get());
      }
      request.setPrevLogIndex(log.getCurrLogIndex() - 1);
      try {
        request.setPrevLogTerm(raftMember.getLogManager().getTerm(log.getCurrLogIndex() - 1));
      } catch (Exception e) {
        logger.error("getTerm failed for newly append entries", e);
      }

      handler.setLog(log);
      request.setEntry(log.serialize());
      logger.debug("Catching up {} with log {}", node, log);

      synchronized (appendSucceed) {
        AsyncClient client = raftMember.connectNode(node);
        if (client == null) {
          return;
        }
        client.appendEntry(request, handler);
        raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
        appendSucceed.wait(RaftServer.connectionTimeoutInMS);
      }
      abort = !appendSucceed.get();
    }
  }

  void doLogCatchUpInBatch() throws TException, InterruptedException {
    AppendEntriesRequest request = new AppendEntriesRequest();
    AtomicBoolean appendSucceed = new AtomicBoolean(false);
    boolean abort = false;

    LogCatchUpInBatchHandler handler = new LogCatchUpInBatchHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setRaftMember(raftMember);
    handler.setFollower(node);
    if (raftMember.getHeader() != null) {
      request.setHeader(raftMember.getHeader());
    }

    for (int i = 0; i < logs.size() && !abort; i += LOG_NUM_IN_BATCH) {
      List<ByteBuffer> logList = new ArrayList<>();
      for (int j = i; j < i + LOG_NUM_IN_BATCH && j < logs.size(); j++) {
        logList.add(logs.get(j).serialize());
      }
      synchronized (raftMember.getTerm()) {
        // make sure this node is still a leader
        if (raftMember.getCharacter() != NodeCharacter.LEADER) {
          logger.debug("Leadership is lost when doing a catch-up to {}, aborting", node);
          break;
        }
        request.setTerm(raftMember.getTerm().get());
      }

      handler.setLogs(logList);
      request.setEntries(logList);
      // set index for raft
      request.setLeader(raftMember.getThisNode());
      request.setPrevLogIndex(logs.get(i).getCurrLogIndex() - 1);
      long commitIndex;
      synchronized (raftMember.getLogManager()) {
        commitIndex = raftMember.getLogManager().getCommitLogIndex();
      }
      request.setLeaderCommit(commitIndex);
      try {
        request
            .setPrevLogTerm(raftMember.getLogManager().getTerm(logs.get(i).getCurrLogIndex() - 1));
      } catch (Exception e) {
        logger.error("getTerm failed for newly append entries", e);
      }
      logger.debug("Catching up {} with log {}", node, logList);

      // do append entries
      synchronized (appendSucceed) {
        AsyncClient client = raftMember.connectNode(node);
        if (client == null) {
          return;
        }

        client.appendEntries(request, handler);
        raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
        appendSucceed.wait(RaftServer.connectionTimeoutInMS);
      }
      abort = !appendSucceed.get();
    }
  }

  public void run() {
    try {
      if (useBatch) {
        doLogCatchUpInBatch();
      } else {
        doLogCatchUp();
      }
    } catch (Exception e) {
      logger.error("Catch up {} errored", node, e);
    }
    logger.debug("Catch up {} finished", node);
    // the next catch up is enabled
    raftMember.getLastCatchUpResponseTime().remove(node);
  }
}
