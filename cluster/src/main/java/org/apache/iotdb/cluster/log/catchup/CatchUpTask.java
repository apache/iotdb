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
import java.util.ConcurrentModificationException;
import java.util.List;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
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
   * @return true if a matched index is found so that we can use logs only to catch up, or false if
   * the catch up must be done with a snapshot.
   * @throws TException
   * @throws InterruptedException
   */
  boolean checkMatchIndex() throws TException, InterruptedException, LeaderUnknownException {
    boolean isLogDebug = logger.isDebugEnabled();
    long lo = 0;
    long hi = 0;
    logger.debug("Checking the match index of {}", node);
    try {
      long localFirstIndex = raftMember.getLogManager().getFirstIndex();
      lo = Math.max(localFirstIndex, peer.getMatchIndex() + 1);
      hi = raftMember.getLogManager().getLastLogIndex() + 1;
      logs = raftMember.getLogManager().getEntries(lo, hi);
      // this may result from peer's match index being changed concurrently, making the peer
      // actually catch up now
      if (logs.isEmpty()) {
        return true;
      }
      if (isLogDebug) {
        logger.debug(
            "{}: use {} logs of [{}, {}] to fix log inconsistency with node [{}], "
                + "local first index: {}",
            raftMember.getName(), logs.size(), lo, hi, node, localFirstIndex);
      }
    } catch (ConcurrentModificationException e) {
      // ignore
    } catch (Exception e) {
      logger.error("Unexpected error in logManager's getEntries during matchIndexCheck", e);
    }


    int index = logs.size() - 1;
    // if index < 0 then send Snapshot and all the logs in logManager
    // if index >= 0 but there is no matched log, still send Snapshot and all the logs in logManager
    while (index >= 0) {
      if (checkMatchIndex(index)) {
        logger.debug("{}: Find a match index {} of {}", raftMember.getName(), index, node);
        return true;
      }
      index--;
    }
    logger.info("Cannot find matched of {} within [{}, {}]", node, lo, hi);
    return false;
  }

  /**
   * @param index the index of a log in logs
   * @return true if the log at logs[index] matches a log in the remote node, false if the
   * corresponding log cannot be found
   * @throws LeaderUnknownException
   * @throws TException
   * @throws InterruptedException
   */
  private boolean checkMatchIndex(int index)
      throws LeaderUnknownException, TException, InterruptedException {
    boolean isLogDebug = logger.isDebugEnabled();
    Log log = logs.get(index);
    synchronized (raftMember.getTerm()) {
      // make sure this node is still a leader
      if (raftMember.getCharacter() != NodeCharacter.LEADER) {
        throw new LeaderUnknownException(raftMember.getAllNodes());
      }
    }

    long prevLogIndex = log.getCurrLogIndex() - 1;
    long prevLogTerm = getPrevLogTerm(index);

    if (prevLogTerm == -1) {
      // prev log cannot be found, we cannot know whether is matches
      return false;
    }

    boolean matched;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      RaftService.AsyncClient client = raftMember.getAsyncClient(node);
      matched = SyncClientAdaptor
          .matchTerm(client, node, prevLogIndex, prevLogTerm, raftMember.getHeader());
    } else {
      Client client = raftMember.getSyncClient(node);
      try {
        matched = client.matchTerm(prevLogIndex, prevLogTerm, raftMember.getHeader());
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        throw e;
      } finally {
        raftMember.putBackSyncClient(client);
      }
    }

    raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
    logger.debug("{} check {}'s matchIndex {} with log [{}]", raftMember.getName(), node,
        matched ? "succeed" : "failed", log);
    if (matched) {
      // if follower return RESPONSE.AGREE with this empty log, then start sending real logs from index.
      logs.subList(0, index).clear();
      if (isLogDebug) {
        if (logs.isEmpty()) {
          logger.debug("{}: {} has caught up by previous catch up", raftMember.getName(), node);
        } else {
          logger.debug("{}: makes {} catch up with {} and other {} logs", raftMember.getName(),
              node, logs.get(0), logs.size());
        }
      }
      return true;
    }
    return false;
  }

  private long getPrevLogTerm(int index) {
    long prevLogTerm = -1;
    if (index > 0) {
      prevLogTerm = logs.get(index - 1).getCurrLogTerm();
    } else {
      try {
        prevLogTerm = raftMember.getLogManager().getTerm(logs.get(0).getCurrLogIndex() - 1);
      } catch (EntryCompactedException e) {
        logger.info("Log [{}] is compacted during catchup", logs.get(0).getCurrLogIndex() - 1);
      }
    }
    return prevLogTerm;
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
      boolean findMatchedIndex = checkMatchIndex();
      boolean catchUpSucceeded;
      if (!findMatchedIndex) {
        doSnapshot();
        SnapshotCatchUpTask task = new SnapshotCatchUpTask(logs, snapshot, node, raftMember);
        catchUpSucceeded = task.call();
      } else {
        LogCatchUpTask task = new LogCatchUpTask(logs, node, raftMember);
        catchUpSucceeded = task.call();
      }
      if (catchUpSucceeded) {
        // the catch up may be triggered by an old heartbeat, and the node may have already
        // caught up, so logs can be empty
        if (!logs.isEmpty()) {
          peer.setMatchIndex(logs.get(logs.size() - 1).getCurrLogIndex());
          // update peer's status so raftMember can send logs in main thread.
          peer.setCatchUp(true);
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Catch up {} finished, update it's matchIndex to {}",
                raftMember.getName(), node,
                logs.get(logs.size() - 1).getCurrLogIndex());
          }
        } else {
          logger.debug("{}: Logs are empty when catching up {}, it may have been caught up",
              raftMember.getName(), node);
        }

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
