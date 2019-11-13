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

package org.apache.iotdb.cluster.log;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import org.apache.iotdb.cluster.log.meta.AddNodeLog;

// TODO-Cluster: implement a serializable LogManager
public class MemoryLogManager implements LogManager {

  private long commitLogIndex = -1;

  private Deque<Log> logBuffer = new ArrayDeque<>();
  private LogApplier logApplier;

  public MemoryLogManager(LogApplier logApplier) {
    this.logApplier = logApplier;
  }

  @Override
  public long getLastLogIndex() {
    return logBuffer.isEmpty() ? -1 : logBuffer.getLast().getCurrLogIndex();
  }

  @Override
  public long getLastLogTerm() {
    return logBuffer.isEmpty() ? -1 : logBuffer.getLast().getCurrLogTerm();
  }

  @Override
  public long getCommitLogIndex() {
    return commitLogIndex;
  }

  @Override
  public void appendLog(Log log) {
    logBuffer.addLast(log);
    if (log instanceof AddNodeLog) {
      // AddNodeLog should be applied instantly as it requires strong consistency
      // Notice: applying AddNodeLog twice does not induce side effect
      logApplier.apply(log);
    }
  }

  @Override
  public void removeLastLog() {
    if (!logBuffer.isEmpty()) {
      logBuffer.removeLast();
    }
  }

  @Override
  public void replaceLastLog(Log log) {
    logBuffer.removeLast();
    logBuffer.addLast(log);
    if (log instanceof AddNodeLog) {
      // AddNodeLog should be applied instantly as it requires strong consistency
      // Notice: applying AddNodeLog twice does not induce side effect
      logApplier.apply(log);
    }
  }

  @Override
  public void commitLog(long maxLogIndex) {
    for (Log log : logBuffer) {
      long i  = log.getCurrLogIndex();
      if (commitLogIndex < i && i <= maxLogIndex) {
        logApplier.apply(log);
        commitLogIndex++;
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
      if (startIndex <= i && i <= endIndex) {
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
  public Snapshot getSnapshot() {
    return null;
  }

  @Override
  public Log getLastLog() {
    return logBuffer.isEmpty()? null : logBuffer.getLast();
  }
}
