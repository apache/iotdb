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

import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Peer;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.utils.TestOnly;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;

public class CatchUpTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(CatchUpTask.class);

  private Node node;
  private Peer peer;
  private RaftMember raftMember;
  private Snapshot snapshot;
  private List<Log> logs;
  private long lastLogIndex;
  private boolean abort;
  private String name;
  private int raftId;

  public CatchUpTask(Node node, int raftId, Peer peer, RaftMember raftMember, long lastLogIdx) {
    this.node = node;
    this.raftId = raftId;
    this.peer = peer;
    this.raftMember = raftMember;
    this.logs = Collections.emptyList();
    this.snapshot = null;
    this.lastLogIndex = lastLogIdx;
    this.name = raftMember.getName() + "@" + System.currentTimeMillis();
  }

  /**
   * @return true if a matched index is found so that we can use logs only to catch up, or false if
   *     the catch up must be done with a snapshot.
   * @throws TException
   * @throws InterruptedException
   */
  private boolean checkMatchIndex()
      throws TException, InterruptedException, LeaderUnknownException {

    logger.debug("Checking the match index of {}", node);
    long lo = 0;
    long hi = 0;
    long localFirstIndex = 0;
    try {
      // to avoid snapshot catch up when index is volatile
      synchronized (raftMember.getLogManager()) {
        localFirstIndex = raftMember.getLogManager().getFirstIndex();
        lo = Math.max(localFirstIndex, peer.getMatchIndex() + 1);
        hi = raftMember.getLogManager().getLastLogIndex() + 1;
        logs = raftMember.getLogManager().getEntries(lo, hi);
      }
      // this may result from peer's match index being changed concurrently, making the peer
      // actually catch up now
      if (logger.isInfoEnabled()) {
        logger.info(
            "{}: use {} logs of [{}, {}] to fix log inconsistency with node [{}], "
                + "local first index: {}",
            raftMember.getName(),
            logs.size(),
            lo,
            hi,
            node,
            localFirstIndex);
      }
    } catch (ConcurrentModificationException e) {
      // ignore
    } catch (Exception e) {
      logger.error("Unexpected error in logManager's getEntries during matchIndexCheck", e);
    }

    if (logs.isEmpty()) {
      return true;
    }

    int index = findLastMatchIndex(logs);
    if (index == -1) {
      logger.info("{}, Cannot find matched of {} within [{}, {}] in memory", name, node, lo, hi);
      if (!judgeUseLogsInDiskToCatchUp()) {
        return false;
      }
      long startIndex = peer.getMatchIndex() + 1;
      long endIndex = raftMember.getLogManager().getCommitLogIndex();
      List<Log> logsInDisk = getLogsInStableEntryManager(startIndex, endIndex);
      if (!logsInDisk.isEmpty()) {
        logger.info(
            "{}, found {} logs in disk to catch up {} , startIndex={}, endIndex={}, memoryFirstIndex={}, getFirstLogIndex={}",
            name,
            logsInDisk.size(),
            node,
            startIndex,
            endIndex,
            localFirstIndex,
            logsInDisk.get(0).getCurrLogIndex());
        logs = logsInDisk;
        index = findLastMatchIndex(logs);
        // the follower's matchIndex may have been updated
        if (index == -1) {
          return false;
        }
      } else {
        logger.info(
            "{}, Cannot find matched of {} within [{}, {}] in disk",
            name,
            node,
            startIndex,
            endIndex);
        return false;
      }
    }
    long newMatchedIndex = logs.get(index).getCurrLogIndex() - 1;
    if (newMatchedIndex > lastLogIndex) {
      logger.info(
          "{}: matched index of {} has moved beyond last log index, node is "
              + "self-catching-up, abort this catch up to avoid duplicates",
          name,
          node);
      abort = true;
      return true;
    }
    logger.info("{}: {} matches at {}", name, node, newMatchedIndex);

    peer.setMatchIndex(newMatchedIndex);
    // if follower return RESPONSE.AGREE with this empty log, then start sending real logs from
    // index.
    logs.subList(0, index).clear();
    if (logger.isInfoEnabled()) {
      if (logs.isEmpty()) {
        logger.info("{}: {} has caught up by previous catch up", name, node);
      } else {
        logger.info(
            "{}: makes {} catch up with {} and other {} logs",
            name,
            node,
            logs.get(0),
            logs.size());
      }
    }
    return true;
  }

  @SuppressWarnings("squid:S1135")
  private boolean judgeUseLogsInDiskToCatchUp() {
    // TODO use log in disk to snapshot first, if the log not found on disk, then use snapshot.
    if (!ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence()) {
      return false;
    }
    // TODO judge the cost of snapshot and logs in disk
    return ClusterDescriptor.getInstance().getConfig().isEnableUsePersistLogOnDiskToCatchUp();
  }

  private List<Log> getLogsInStableEntryManager(long startIndex, long endIndex) {
    List<Log> logsInDisk =
        raftMember.getLogManager().getStableEntryManager().getLogs(startIndex, endIndex);
    logger.debug(
        "{}, found {} logs in disk to catchup {}, startIndex={}, endIndex={}",
        raftMember.getName(),
        logsInDisk.size(),
        node,
        startIndex,
        endIndex);
    return logsInDisk;
  }

  /**
   * return the index of log whose previous log is matched, or -1 when can not found
   *
   * @param logs
   * @return
   * @throws LeaderUnknownException
   * @throws TException
   * @throws InterruptedException
   */
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
   * @return true if the previous log at logs[index] matches a log in the remote node, false if the
   *     corresponding log cannot be found
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
      // prev log cannot be found, we cannot know whether is matches if it is not the first log
      return prevLogIndex == -1;
    }

    boolean matched = checkLogIsMatch(prevLogIndex, prevLogTerm);
    raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
    logger.info(
        "{} check {}'s matchIndex {} with log [{}]",
        raftMember.getName(),
        node,
        matched ? "succeed" : "failed",
        log);
    return matched;
  }

  /**
   * @param logIndex the log index needs to check
   * @param logTerm the log term need to check
   * @return true if the log's index and term matches a log in the remote node, false if the
   *     corresponding log cannot be found
   * @throws TException
   * @throws InterruptedException
   */
  private boolean checkLogIsMatch(long logIndex, long logTerm)
      throws TException, InterruptedException {
    boolean matched;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      RaftService.AsyncClient client = raftMember.getAsyncClient(node);
      if (client == null) {
        return false;
      }
      matched =
          SyncClientAdaptor.matchTerm(client, node, logIndex, logTerm, raftMember.getHeader());
    } else {
      Client client = raftMember.getSyncClient(node);
      if (client == null) {
        return false;
      }
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
    snapshot = raftMember.getLogManager().getSnapshot(peer.getMatchIndex());
    if (logger.isInfoEnabled()) {
      logger.info("{}: Logs in {} are too old, catch up with snapshot", raftMember.getName(), node);
    }
  }

  /** Remove logs that are contained in the snapshot. */
  private void removeSnapshotLogs() {
    Log logToSearch = new EmptyContentLog(snapshot.getLastLogIndex(), snapshot.getLastLogTerm());
    int pos =
        Collections.binarySearch(logs, logToSearch, Comparator.comparingLong(Log::getCurrLogIndex));
    int prevSize = logs.size();
    if (pos >= 0) {
      logs.subList(0, pos + 1).clear();
    } else {
      int insertPos = -pos - 1;
      if (insertPos > 0) {
        logs.subList(0, insertPos).clear();
      }
    }
    logger.info("Logs are reduced from {} to {}", prevSize, logs.size());
  }

  @Override
  public void run() {
    try {
      boolean findMatchedIndex = checkMatchIndex();
      if (abort) {
        peer.resetInconsistentHeartbeatNum();
        raftMember.getLastCatchUpResponseTime().remove(node);
        return;
      }
      boolean catchUpSucceeded;
      if (!findMatchedIndex) {
        logger.info("{}: performing a snapshot catch-up to {}", raftMember.getName(), node);
        doSnapshot();
        // snapshot may overlap with logs
        removeSnapshotLogs();
        SnapshotCatchUpTask task =
            new SnapshotCatchUpTask(logs, snapshot, node, raftId, raftMember);
        catchUpSucceeded = task.call();
      } else {
        logger.info("{}: performing a log catch-up to {}", raftMember.getName(), node);
        LogCatchUpTask task = new LogCatchUpTask(logs, node, raftId, raftMember);
        catchUpSucceeded = task.call();
      }
      if (catchUpSucceeded) {
        // the catch up may be triggered by an old heartbeat, and the node may have already
        // caught up, so logs can be empty
        if (!logs.isEmpty() || snapshot != null) {
          long lastIndex =
              !logs.isEmpty()
                  ? logs.get(logs.size() - 1).getCurrLogIndex()
                  : snapshot.getLastLogIndex();
          peer.setMatchIndex(lastIndex);
        }
        if (logger.isInfoEnabled()) {
          logger.info(
              "{}: Catch up {} finished, update it's matchIndex to {}",
              raftMember.getName(),
              node,
              peer.getMatchIndex());
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
