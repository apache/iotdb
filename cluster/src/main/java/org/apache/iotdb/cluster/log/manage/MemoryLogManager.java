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

package org.apache.iotdb.cluster.log.manage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.manage.serializable.LogManagerMeta;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO-Cluster#351: implement a serializable LogManager.

/**
 * MemoryLogManager stores all logs in a memory deque without providing snapshots.
 */
public abstract class MemoryLogManager implements LogManager {

  private static final Logger logger = LoggerFactory.getLogger(MemoryLogManager.class);
  protected long lastLogId = -1;
  protected long lastLogTerm = -1;
  protected LogApplier logApplier;
  long commitLogIndex = -1;
  List<Log> logBuffer = new ArrayList<>();
  protected MemoryLogManager(LogApplier logApplier) {
    this.logApplier = logApplier;
  }

  public void setLogBuffer(List<Log> logBuffer) {
    this.logBuffer = logBuffer;
  }

  public long getLastLogId() {
    return lastLogId;
  }

  @Override
  public void setLastLogId(long lastLogId) {
    this.lastLogId = lastLogId;
  }

  @Override
  public long getLastLogIndex() {
    return lastLogId;
  }

  @Override
  public long getLastLogTerm() {
    return lastLogTerm;
  }

  @Override
  public void setLastLogTerm(long lastLogTerm) {
    this.lastLogTerm = lastLogTerm;
  }

  @Override
  public long getCommitLogIndex() {
    return commitLogIndex;
  }

  public void setCommitLogIndex(long commitLogIndex) {
    this.commitLogIndex = commitLogIndex;
  }

  @Override
  public boolean appendLog(Log appendingLog) {
    long appendingPrevIndex = appendingLog.getPreviousLogIndex();
    long appendingPrevTerm = appendingLog.getPreviousLogTerm();
    long appendingCurrTerm = appendingLog.getCurrLogTerm();
    long appendingCurrIndex = appendingLog.getCurrLogIndex();
    if (logBuffer.isEmpty()) {
      // the logs are empty, check the appendingLog with the recorded last log index and term
      if (appendingPrevIndex == lastLogId && appendingPrevTerm == lastLogTerm) {
        logBuffer.add(appendingLog);
        lastLogId = appendingLog.getCurrLogIndex();
        lastLogTerm = appendingLog.getCurrLogTerm();
        return true;
      } else {
        return false;
      }
    }

    // find the first log whose index <= appendingLog's
    int insertPos = logBuffer.size() - 1;
    for (; insertPos >= 0; insertPos--) {
      Log currLog = logBuffer.get(insertPos);
      if (currLog.getCurrLogIndex() == appendingCurrIndex
          && currLog.getCurrLogTerm() == appendingCurrTerm) {
        // the log is already appended
        return true;
      }

      if (currLog.getCurrLogIndex() == appendingPrevIndex) {
        if (appendingPrevTerm != currLog.getCurrLogTerm()) {
          // log mismatch
          return false;
        } else {
          insertPos++;
          break;
        }
      } else if (currLog.getCurrLogIndex() < appendingPrevIndex) {
        // log mismatch
        return false;
      }
      // continue to the previous log if currLog's index > appendingLog's
    }
    // if the all logs' indices are larger than appendingLog's, just clear the buffer
    insertPos = Math.max(insertPos, 0);
    // the insertion position is found, insert the log into insertPos and discard the following logs
    logBuffer.subList(insertPos, logBuffer.size()).clear();
    logBuffer.add(appendingLog);
    lastLogId = appendingLog.getCurrLogIndex();
    lastLogTerm = appendingLog.getCurrLogTerm();
    return true;
  }

  @Override
  public synchronized void commitLog(long maxLogIndex) {
    if (maxLogIndex <= commitLogIndex) {
      return;
    }
    for (Log log : logBuffer) {
      long i = log.getCurrLogIndex();
      if (commitLogIndex < i && i <= maxLogIndex) {
        try {
          logApplier.apply(log);
        } catch (QueryProcessException e) {
          if (!(e.getCause() instanceof PathAlreadyExistException)) {
            logger.error("Cannot apply a log {} in snapshot, ignored", log, e);
          }
        }
        commitLogIndex = i;
      }
    }
  }

  @Override
  public List<Log> getLogs(long startIndex, long endIndex) {
    if (startIndex > endIndex) {
      return Collections.emptyList();
    }

    List<Log> ret = new ArrayList<>();
    for (Log log : logBuffer) {
      long i = log.getCurrLogIndex();
      if (startIndex <= i && i < endIndex) {
        ret.add(log);
      }
    }
    return ret;
  }

  @Override
  public boolean logValid(long logIndex) {
    return !logBuffer.isEmpty() && logBuffer.get(0).getCurrLogIndex()
        <= logIndex && logIndex <= logBuffer.get(logBuffer.size() - 1).getCurrLogIndex();
  }

  @Override
  public Log getLastLog() {
    return logBuffer.isEmpty() ? null : logBuffer.get(logBuffer.size() - 1);
  }

  @Override
  public LogApplier getApplier() {
    return logApplier;
  }

  public void removeFromHead(int length) {
    logBuffer.subList(0, length).clear();
  }

  @TestOnly
  public LogManagerMeta getMeta() {
    LogManagerMeta managerMeta = new LogManagerMeta();
    managerMeta.setCommitLogIndex(commitLogIndex);
    managerMeta.setLastLogId(lastLogId);
    managerMeta.setLastLogTerm(lastLogTerm);
    return managerMeta;
  }

  @TestOnly
  public void setMeta(LogManagerMeta meta) {
    commitLogIndex = meta.getCommitLogIndex();
    lastLogId = meta.getLastLogId();
    lastLogTerm = meta.getLastLogTerm();
  }
}
