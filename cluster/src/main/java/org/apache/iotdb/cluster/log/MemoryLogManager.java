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
import java.util.Iterator;
import java.util.List;

// TODO-Cluster: implement a serializable LogManager
public class MemoryLogManager implements LogManager {

  private long firstLogIndex = 0;
  private long lastLogIndex = -1;
  private long lastLogTerm = -1;
  private long commitLogIndex = -1;

  private Deque<Log> logBuffer = new ArrayDeque<>();
  private LogApplier logApplier;

  public MemoryLogManager(LogApplier logApplier) {
    this.logApplier = logApplier;
  }

  @Override
  public long getLastLogIndex() {
    return lastLogIndex;
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
  public void appendLog(Log log, long term) {
    logBuffer.addLast(log);
    lastLogTerm = term;
    lastLogIndex ++;
  }

  @Override
  public void replaceLastLog(Log log, long term) {
    logBuffer.removeLast();
    logBuffer.addLast(log);
    lastLogTerm = term;
  }

  @Override
  public void commitLog(long maxLogIndex) {
    Iterator<Log> logIterator = logBuffer.iterator();
    for (long i = firstLogIndex; i <= lastLogIndex; i++) {
      Log currLog = logIterator.next();
      if (commitLogIndex < i && i <= maxLogIndex) {
        logApplier.apply(currLog);
        commitLogIndex ++;
      }
    }
  }

  @Override
  public List<Log> getLogs(long startIndex, long endIndex) {
    if (startIndex > endIndex) {
      return Collections.emptyList();
    }

    Iterator<Log> logIterator = logBuffer.iterator();
    List<Log> ret = new ArrayList<>();
    for (long i = firstLogIndex; i <= lastLogIndex; i++) {
      Log currLog = logIterator.next();
      if (startIndex <= i && i <= endIndex) {
        ret.add(currLog);
      }
    }
    return ret;
  }

  @Override
  public boolean logValid(long logIndex) {
    return firstLogIndex <= logIndex && logIndex <= lastLogIndex;
  }

  @Override
  public Snapshot getSnapshot() {
    return null;
  }
}
