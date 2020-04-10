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

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;

/**
 * LogManager manages the logs that are still in memory and the last snapshot which can be used
 * to make other nodes catch up.
 */
public interface LogManager {

  long getLastLogIndex();
  long getLastLogTerm();
  long getCommitLogIndex();

  /**
   * Append log to a proper place in the log chain.
   * If the previous log of the appending log can be found, the new log will be appended after
   * the log and old logs before the log will be removed and a true will be returned.
   * Otherwise this method will return false.
   * @param log
   * @return true if the log is successfully appended, false otherwise.
   */
  boolean appendLog(Log log);

  /**
   * Commit (apply) all memory logs whose index <= maxLogIndex, also change commit log index.
   * @param maxLogIndex
   */
  void commitLog(long maxLogIndex);

  /**
   * Get all logs whose index in [startIndex, endIndex).
   * @param startIndex
   * @param endIndex
   * @return logs whose index in [startIndex, endIndex).
   */
  List<Log> getLogs(long startIndex, long endIndex);

  /**
   * Test whether a log whose index is logIndex is still in memory.
   * @param logIndex
   * @return true if the log is still in memory, false if the log has been snapshot.
   */
  boolean logValid(long logIndex);

  /**
   * Get the latest snapshot.
   * @return the latest snapshot, or null if there is no snapshot.
   */
  Snapshot getSnapshot();

  /**
   * Take a snapshot of the committed logs instantly and discard the committed logs.
   */
  void takeSnapshot() throws IOException;

  LogApplier getApplier();

  void setLastLogId(long lastLogId);

  void setLastLogTerm(long lastLogTerm);

  /**
   * Wait until all remote snapshots are pulled locally.
   */
  default void waitRemoteSnapshots() {

  };
}
