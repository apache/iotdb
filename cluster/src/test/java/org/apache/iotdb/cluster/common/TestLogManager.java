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

package org.apache.iotdb.cluster.common;

import java.util.List;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.Snapshot;

public class TestLogManager implements LogManager {

  private long lastLogIndex;
  private long lastLogTerm;
  private long commitLogIndex;

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
  public void appendLog(Log log) {

  }

  @Override
  public void removeLastLog() {

  }

  @Override
  public void replaceLastLog(Log log) {

  }

  @Override
  public void commitLog(long maxLogIndex) {
    commitLogIndex = Math.max(commitLogIndex, maxLogIndex);
  }

  @Override
  public void commitLog(Log log) {
    commitLog(log.getCurrLogIndex());
  }

  @Override
  public List<Log> getLogs(long startIndex, long endIndex) {
    return null;
  }

  @Override
  public boolean logValid(long logIndex) {
    return false;
  }

  @Override
  public Snapshot getSnapshot() {
    return null;
  }

  @Override
  public Log getLastLog() {
    return null;
  }

  @Override
  public void takeSnapshot() {

  }

  @Override
  public LogApplier getApplier() {
    return null;
  }

  @Override
  public void setLastLogId(long lastLogId) {
    lastLogIndex = lastLogId;
  }

  @Override
  public void setLastLogTerm(long lastLogTerm) {
    this.lastLogTerm = lastLogTerm;
  }

  public void setCommitLogIndex(long commitLogIndex) {
    this.commitLogIndex = commitLogIndex;
  }
}
