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
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.utils.TestOnly;
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
  private boolean checkMatchIndex()
      throws TException, InterruptedException, LeaderUnknownException {
    long lo = 0;
    long hi = 0;
    logger.debug("Checking the match index of {}", node);
    long localFirstIndex = 0;
    try {
      localFirstIndex = raftMember.getLogManager().getFirstIndex();
      lo = Math.max(localFirstIndex, peer.getMatchIndex() + 1);
      hi = raftMember.getLogManager().getLastLogIndex() + 1;
      logs = raftMember.getLogManager().getEntries(lo, hi);
      // this may result from peer's match index being changed concurrently, making the peer
      // actually catch up now
      if (logs.isEmpty()) {
        return true;
      }
      if (logger.isInfoEnabled()) {
        logger.info(
            "{}: use {} logs of [{}, {}] to fix log inconsistency with node [{}], "
                + "local first index: {}",
            raftMember.getName(), logs.size(), lo, hi, node, localFirstIndex);
      }
    } catch (ConcurrentModificationException e) {
      // ignore
    } catch (Exception e) {
      logger.error("Unexpected error in logManager's getEntries during matchIndexCheck", e);
    }

    int index = findLastMatchIndex(logs);
    if (index == -1) {
      logger.info("Cannot find matched of {} within [{}, {}] in memory", node, lo, hi);
      if (judgeUseLogsInDiskToCatchUp()) {
        long startIndex = peer.getMatchIndex() + 1;
        long endIndex = raftMember.getLogManager().getCommitLogIndex();
        List<Log> logsInDisk = getLogsInStableEntryManager(startIndex, endIndex);
        if (!logsInDisk.isEmpty()) {
          logger.info(
              "{}, found {} logs in disk to catch up, startIndex={}, endIndex={}, memoryFirstIndex={}",
              raftMember.getName(), logsInDisk.size(), startIndex, endIndex, localFirstIndex);
          logs = logsInDisk;
          return true;
        }
      }
      return false;
    }

    // if follower return RESPONSE.AGREE with this empty log, then start sending real logs from index.
    logs.subList(0, index).clear();
    if (logger.isDebugEnabled()) {
      if (logs.isEmpty()) {
        logger.debug("{}: {} has caught up by previous catch up", raftMember.getName(), node);
      } else {
        logger.debug("{}: makes {} catch up with {} and other {} logs", raftMember.getName(),
            node, logs.get(0), logs.size());
      }
    }
    return true;
  }


  //TODO use log in disk to snapshot first, if the log not found on disk, then use snapshot.
  private boolean judgeUseLogsInDiskToCatchUp() {
    return true;
  }

  private List<Log> getLogsInStableEntryManager(long startIndex, long endIndex) {
    List<Log> logsInDisk = raftMember.getLogManager().getStableEntryManager()
        .getLogs(startIndex, endIndex);
    logger.debug("{}, found {} logs in disk to catchup, startIndex={}, endIndex={}",
        raftMember.getLogManager(), logsInDisk.size(), startIndex, endIndex);
    return logsInDisk;
  }

  public int findLastMatchIndex(List<Log> logs)
      throws LeaderUnknownException, TException, InterruptedException {
    int start = 0;
    int end = logs.size() - 1;
    int matchedIndex = -1;
    while (start <= end) {
      int mid = start + (end - start) / 2;
      if (checkMatchIndex(mid)) {
        start = mid + 1;
        matchedIndex = mid;
      } else {
        end = mid - 1;
      }
    }
    return matchedIndex;
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

    boolean matched = checkLogIsMatch(prevLogIndex, prevLogTerm);
    raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
    logger.debug("{} check {}'s matchIndex {} with log [{}]", raftMember.getName(), node,
        matched ? "succeed" : "failed", log);
    return matched;
  }

  /**
   * @param logIndex the log index needs to check
   * @param logTerm  the log term need to check
   * @return true if the log's index and term matches a log in the remote node, false if the
   * corresponding log cannot be found
   * @throws TException
   * @throws InterruptedException
   */
  private boolean checkLogIsMatch(long logIndex, long logTerm)
      throws TException, InterruptedException {
    boolean matched = false;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      RaftService.AsyncClient client = raftMember.getAsyncClient(node);
      if (client == null) {
        return false;
      }
      matched = SyncClientAdaptor
          .matchTerm(client, node, logIndex, logTerm, raftMember.getHeader());
    } else {
      Client client = raftMember.getSyncClient(node);
      try {
        matched = client.matchTerm(logIndex, logTerm, raftMember.getHeader());
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        throw e;
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
    }
    return matched;
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

  @Override
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
        peer.resetInconsistentHeartbeatNum();
      }

    } catch (LeaderUnknownException e) {
      logger.warn("Catch up {} failed because leadership is lost", node);
    } catch (Exception e) {
      logger.error("Catch up {} errored", node, e);
    }
    // the next catch up is enabled
    raftMember.getLastCatchUpResponseTime().remove(node);
  }

  @TestOnly
  public void setLogs(List<Log> logs) {
    this.logs = logs;
  }
}
