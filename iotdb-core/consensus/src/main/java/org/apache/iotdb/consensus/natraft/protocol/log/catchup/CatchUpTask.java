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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.client.SyncClientAdaptor;
import org.apache.iotdb.consensus.natraft.exception.LeaderUnknownException;
import org.apache.iotdb.consensus.natraft.protocol.PeerInfo;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.RaftRole;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.EmptyEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.snapshot.Snapshot;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;

public class CatchUpTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(CatchUpTask.class);

  private Peer node;
  private PeerInfo peerInfo;
  private RaftMember raftMember;
  private Snapshot snapshot;
  private List<Entry> logs;
  private long lastLogIndex;
  private boolean abort;
  private String name;

  private long startTime;
  private CatchUpManager catchUpManager;
  private RaftConfig config;

  public CatchUpTask(
      Peer node,
      PeerInfo peerInfo,
      CatchUpManager catchUpManager,
      long lastLogIdx,
      RaftConfig config) {
    this.node = node;
    this.peerInfo = peerInfo;
    this.raftMember = catchUpManager.getMember();
    this.catchUpManager = catchUpManager;
    this.logs = Collections.emptyList();
    this.snapshot = null;
    this.lastLogIndex = lastLogIdx;
    this.name = raftMember.getName() + "@" + System.currentTimeMillis();
    this.config = config;
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
      localFirstIndex = raftMember.getLogManager().getFirstIndex();
      lo = Math.max(localFirstIndex, peerInfo.getMatchIndex() + 1);
      hi = raftMember.getLogManager().getLastLogIndex() + 1;
      logs = raftMember.getLogManager().getEntries(lo, hi);

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
      long startIndex = peerInfo.getMatchIndex() + 1;
      long endIndex = raftMember.getLogManager().getCommitLogIndex();
      List<Entry> logsInDisk = getLogsInStableEntryManager(startIndex, endIndex);
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

    peerInfo.setMatchIndex(newMatchedIndex);
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
    if (!config.isEnableRaftLogPersistence()) {
      return false;
    }
    // TODO judge the cost of snapshot and logs in disk
    return config.isEnableUsePersistLogOnDiskToCatchUp();
  }

  private List<Entry> getLogsInStableEntryManager(long startIndex, long endIndex) {
    List<Entry> logsInDisk =
        raftMember.getLogManager().getStableEntryManager().getEntries(startIndex, endIndex, true);
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
  public int findLastMatchIndex(List<Entry> logs)
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
    Entry log = logs.get(index);
    if (raftMember.getRole() != RaftRole.LEADER) {
      throw new LeaderUnknownException(raftMember.getAllNodes());
    }

    long prevLogIndex = log.getCurrLogIndex() - 1;
    long prevLogTerm = getPrevLogTerm(index);

    if (prevLogTerm == -1) {
      // prev log cannot be found, we cannot know whether is matches if it is not the first log
      return prevLogIndex == -1;
    }

    boolean matched = checkLogIsMatch(prevLogIndex, prevLogTerm);
    catchUpManager.registerTask(node);
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
    AsyncRaftServiceClient client = raftMember.getClient(node.getEndpoint());
    if (client == null) {
      return false;
    }
    matched =
        SyncClientAdaptor.matchTerm(
            client, node.getEndpoint(), logIndex, logTerm, raftMember.getRaftGroupId());
    return matched;
  }

  private long getPrevLogTerm(int index) {
    long prevLogTerm = -1;
    if (index > 0) {
      prevLogTerm = logs.get(index - 1).getCurrLogTerm();
    } else {
      prevLogTerm = raftMember.getLogManager().getTerm(logs.get(0).getCurrLogIndex() - 1);
    }
    return prevLogTerm;
  }

  private void doSnapshot() {
    raftMember.getLogManager().takeSnapshot(raftMember);
    snapshot = raftMember.getLogManager().getSnapshot(peerInfo.getMatchIndex());
    if (logger.isInfoEnabled()) {
      logger.info(
          "{}: Logs in {} are too old, catch up with snapshot {}-{}",
          raftMember.getName(),
          node,
          snapshot.getLastLogIndex(),
          snapshot.getLastLogTerm());
    }
  }

  /** Remove logs that are contained in the snapshot. */
  private void removeSnapshotLogs() {
    Entry logToSearch = new EmptyEntry(snapshot.getLastLogIndex(), snapshot.getLastLogTerm());
    int pos =
        Collections.binarySearch(
            logs, logToSearch, Comparator.comparingLong(Entry::getCurrLogIndex));
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
    startTime = System.currentTimeMillis();
    try {
      boolean findMatchedIndex = checkMatchIndex();
      if (abort) {
        peerInfo.resetInconsistentHeartbeatNum();
        catchUpManager.unregisterTask(node);
        return;
      }
      boolean catchUpSucceeded;
      if (!findMatchedIndex) {
        logger.info("{}: performing a snapshot catch-up to {}", raftMember.getName(), node);
        doSnapshot();
        // snapshot may overlap with logs
        removeSnapshotLogs();
        SnapshotCatchUpTask task =
            new SnapshotCatchUpTask(logs, snapshot, node, catchUpManager, config);
        catchUpSucceeded = task.call();
      } else {
        logger.info("{}: performing a log catch-up to {}", raftMember.getName(), node);
        LogCatchUpTask task = new LogCatchUpTask(logs, node, catchUpManager, config);
        catchUpSucceeded = task.call();
      }
      if (catchUpSucceeded) {
        // the catch-up may be triggered by an old heartbeat, and the node may have already
        // caught up, so logs can be empty
        if (!logs.isEmpty() || snapshot != null) {
          long lastIndex =
              !logs.isEmpty()
                  ? logs.get(logs.size() - 1).getCurrLogIndex()
                  : snapshot.getLastLogIndex();
          peerInfo.setMatchIndex(lastIndex);
        }
        if (logger.isInfoEnabled()) {
          logger.info(
              "{}: Catch up {} finished, update it's matchIndex to {}, time consumption: {}ms",
              raftMember.getName(),
              node,
              peerInfo.getMatchIndex(),
              System.currentTimeMillis() - startTime);
        }
        peerInfo.resetInconsistentHeartbeatNum();
      } else {
        // wait for a while before the next catch-up so that the status of the follower may update
        Thread.sleep(5000);
      }

    } catch (LeaderUnknownException e) {
      logger.warn("Catch up {} failed because leadership is lost", node);
    } catch (Exception e) {
      logger.error("Catch up {} errored", node, e);
    }
    // the next catch-up is enabled
    catchUpManager.unregisterTask(node);
  }

  @TestOnly
  public void setLogs(List<Entry> logs) {
    this.logs = logs;
  }
}
