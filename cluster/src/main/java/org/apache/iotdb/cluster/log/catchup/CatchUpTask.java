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
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchUpTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(CatchUpTask.class);

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

  /**
   * @return true if the matched index exceed the memory log bound so a snapshot is necessary, or
   * false if the catch up can be done using memory logs.
   * @throws TException
   * @throws InterruptedException
   */
  boolean checkMatchIndex() throws TException, InterruptedException, LeaderUnknownException {
    boolean isLogDebug = logger.isDebugEnabled();
    synchronized (raftMember.getLogManager()) {
      peer.setNextIndex(raftMember.getLogManager().getLastLogIndex());
      try {
        long localFirstIndex = raftMember.getLogManager().getFirstIndex();
        long lo = Math.max(localFirstIndex, peer.getMatchIndex() + 1);
        long hi = peer.getNextIndex() + 1;
        logs = raftMember.getLogManager().getEntries(lo, hi);
        if (isLogDebug) {
          logger.debug(
              "node [{}] use {} logs of [{}, {}] to fix log inconsistency with node [{}], local first index: {}",
              raftMember.getName(), node, logs.size(), lo, hi, localFirstIndex);
        }
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
          throw new LeaderUnknownException(raftMember.getAllNodes());
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

      RaftService.AsyncClient client = raftMember.connectNode(node);
      boolean matched = SyncClientAdaptor
          .matchTerm(client, node, prevLogIndex, prevLogTerm, raftMember.getHeader());
      raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
      logger.debug("{} check {}'s matchIndex {} with log [{}]", raftMember.getName(), node,
          matched ? "succeed" : "failed", log);
      if (matched) {
        // if follower return RESPONSE.AGREE with this empty log, then start sending real logs from index.
        logs.subList(0, index).clear();
        if (isLogDebug) {
          logger.debug("{} makes {} catch up with {} cached logs", raftMember.getName(), node,
              logs.size());
        }
        return false;
      }
      index--;
    }
    return true;
  }

  private void doSnapshot() {
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
  }

  public void run() {
    try {
      boolean needSnapshot = checkMatchIndex();
      if (needSnapshot) {
        doSnapshot();
        SnapshotCatchUpTask task = new SnapshotCatchUpTask(logs, snapshot, node, raftMember);
        task.call();
      } else {
        LogCatchUpTask task = new LogCatchUpTask(logs, node, raftMember);
        task.call();
      }
      // there must be at least one log if catchUp is called.
      peer.setMatchIndex(logs.get(logs.size() - 1).getCurrLogIndex());
      // update peer's status so raftMember can send logs in main thread.
      peer.setCatchUp(true);
      if (logger.isDebugEnabled()) {
        logger.debug("Catch up {} finished, update it's matchIndex to {}", node,
            logs.get(logs.size() - 1).getCurrLogIndex());
      }
    } catch (LeaderUnknownException e) {
      logger.warn("Catch up {} failed because leadership is lost", node);
    } catch (Exception e) {
      logger.error("Catch up {} errored", node, e);
    }
    // the next catch up is enabled
    raftMember.getLastCatchUpResponseTime().remove(node);
  }
}
