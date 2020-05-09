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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.caller.LogCatchUpHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchUpTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(LogCatchUpTask.class);

  private Node node;
  private Peer peer;
  private RaftMember raftMember;
  private Snapshot snapshot;
  private List<Log> logs;


  public CatchUpTask(Node node, Peer peer, RaftMember raftMember) {
    this.node = node;
    this.peer = peer;
    this.raftMember = raftMember;
    this.logs = Collections.emptyList();
    this.snapshot = null;
  }

  boolean checkMatchIndex() throws TException, InterruptedException {

    AtomicReference<Boolean> resultRef = new AtomicReference<>();
    GenericHandler matchTermHandler = new GenericHandler(node, resultRef);

    synchronized (raftMember.getLogManager()) {
      peer.setNextIndex(raftMember.getLogManager().getLastLogIndex());
      try {
        long lo = Math.max(raftMember.getLogManager().getFirstIndex(), peer.getMatchIndex() + 1);
        long hi = peer.getNextIndex() + 1;
        logs = raftMember.getLogManager().getEntries(lo, hi);
        logger.debug("Get {} logs of [{}, {}]to check match index", logs.size(), lo, hi);
      } catch (Exception e) {
        logger.error("Unexpected error in logManager's getEntries during matchIndexCheck", e);
      }
    }

    int index = logs.size() - 1;
    while (index >= 0) {
      Log log = logs.get(index);
      synchronized (raftMember.getTerm()) {
        // make sure this node is still a leader
        if (raftMember.getCharacter() != NodeCharacter.LEADER) {
          logger.debug("Leadership is lost when doing a catch-up to {}, aborting", node);
          break;
        }
      }
      long prevLogIndex = log.getCurrLogIndex() - 1;
      long prevLogTerm = -1;
      if (index > 0) {
        prevLogTerm = logs.get(index - 1).getCurrLogTerm();
      } else {
        try {
          prevLogTerm = raftMember.getLogManager().getTerm(log.getCurrLogIndex() - 1);
        } catch (Exception e) {
          logger.error("getTerm failed for newly append entries", e);
        }
      }

      synchronized (resultRef) {
        AsyncClient client = raftMember.connectNode(node);
        if (client == null) {
          break;
        }
        client.matchTerm(prevLogIndex, prevLogTerm, raftMember.getHeader(), matchTermHandler);
        raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
        resultRef.wait(RaftServer.connectionTimeoutInMS);
      }
      if (resultRef.get()) {
        // if follower return RESPONSE.AGREE with this empty log, then start sending real logs from index.
        logs.subList(0, index).clear();
        if (logger.isDebugEnabled()) {
          logger.debug("{} makes {} catch up with {} cached logs", raftMember.getName(), node,
              logs.size());
        }
        return false;
      }
      index--;
    }
    try {
      raftMember.getLogManager().takeSnapshot();
    } catch (IOException e) {
      logger.error("Unexpected error when taking snapshot.", e);
    }
    snapshot = raftMember.getLogManager().getSnapshot();
    if (logger.isDebugEnabled()) {
      logger
          .debug("{}: Logs in {} are too old, catch up with snapshot", raftMember.getName(), node);
    }
    return true;
  }

  public void run() {
    try {
      if (checkMatchIndex()) {
        SnapshotCatchUpTask task = new SnapshotCatchUpTask(logs, snapshot, node, raftMember);
        task.run();
      } else {
        LogCatchUpTask task = new LogCatchUpTask(logs, node, raftMember);
        task.run();
      }
      // there must be at least one log if catchUp is called.
      peer.setMatchIndex(logs.get(logs.size() - 1).getCurrLogIndex());
      // update peer's status so raftMember can send logs in main thread.
      peer.setCatchUp(true);
      logger.debug("Catch up {} finished, update it's matchIndex to {}", node,
          logs.get(logs.size() - 1).getCurrLogIndex());
    } catch (Exception e) {
      logger.error("Catch up {} errored", node, e);
    }
    // the next catch up is enabled
    raftMember.getLastCatchUpResponseTime().remove(node);
  }
}
