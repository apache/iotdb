/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.manage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO-Cluster#351: implement a serializable LogManager.
/**
 * MemoryLogManager stores all logs in a memory deque without providing snapshots.
 */
public abstract class MemoryLogManager implements LogManager {

  private static final Logger logger = LoggerFactory.getLogger(MemoryLogManager.class);

  long commitLogIndex = -1;
  private long lastLogId = -1;
  private long lastLogTerm = -1;

  Deque<Log> logBuffer = new ArrayDeque<>();
  private LogApplier logApplier;

  MemoryLogManager(LogApplier logApplier) {
    this.logApplier = logApplier;
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
  public long getCommitLogIndex() {
    return commitLogIndex;
  }

  @Override
  public void appendLog(Log log) {
    logBuffer.addLast(log);
    lastLogId = log.getCurrLogIndex();
    lastLogTerm = log.getCurrLogTerm();
  }

  @Override
  public void removeLastLog() {
    if (!logBuffer.isEmpty()) {
      Log log = logBuffer.removeLast();
      lastLogId = log.getPreviousLogIndex();
      lastLogTerm = log.getPreviousLogTerm();
    }
  }

  @Override
  public void replaceLastLog(Log log) {
    logBuffer.removeLast();
    logBuffer.addLast(log);
    lastLogId = log.getCurrLogIndex();
    lastLogTerm = log.getCurrLogTerm();
  }

  @Override
  public synchronized void commitLog(long maxLogIndex) {
    if (maxLogIndex <= commitLogIndex) {
      return;
    }
    for (Log log : logBuffer) {
      long i  = log.getCurrLogIndex();
      if (commitLogIndex < i && i <= maxLogIndex) {
        try {
          logApplier.apply(log);
        } catch (QueryProcessException e) {
          logger.error("Cannot apply a log {} in snapshot, ignored", log, e);
        }
        commitLogIndex = i;
      }
    }
  }

  @Override
  public void commitLog(Log log) throws QueryProcessException {
    logApplier.apply(log);
    commitLogIndex = log.getCurrLogIndex();
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
    return !logBuffer.isEmpty() && logBuffer.getFirst().getCurrLogIndex()
        <= logIndex && logIndex <= logBuffer.getLast().getCurrLogIndex();
  }

  @Override
  public Log getLastLog() {
    return logBuffer.isEmpty()? null : logBuffer.getLast();
  }

  @Override
  public LogApplier getApplier() {
    return logApplier;
  }

  @Override
  public void setLastLogId(long lastLogId) {
    this.lastLogId = lastLogId;
  }

  @Override
  public void setLastLogTerm(long lastLogTerm) {
    this.lastLogTerm = lastLogTerm;
  }
}
