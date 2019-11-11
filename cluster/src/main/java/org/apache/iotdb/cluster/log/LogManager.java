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

import java.util.List;

public interface LogManager {

  long getLastLogIndex();
  long getLastLogTerm();
  long getCommitLogIndex();

  /**
   * Append log to the last of logs, also increase last log index and change last log term.
   * @param log
   */
  void appendLog(Log log, long term);

  /**
   * Remove the last log, also decrease last log index and change last log term.
   */
  void removeLastLog();

  /**
   * Replace the last log the given log, change last log term but do not change last log index.
   * @param log
   */
  void replaceLastLog(Log log, long term);

  /**
   * Commit all logs whose index <= maxLogIndex, also change commit log index.
   * @param maxLogIndex
   */
  void commitLog(long maxLogIndex);

  /**
   * Get all logs whose index in [startIndex, endIndex].
   * @param startIndex
   * @param endIndex
   * @return logs whose index in [startIndex, endIndex].
   */
  List<Log> getLogs(long startIndex, long endIndex);

  /**
   * Test whether a log whose index is logIndex is still available.
   * @param logIndex
   * @return true if the log is still available, false if the log has been deleted.
   */
  boolean logValid(long logIndex);

  /**
   *
   * @return the latest snapshot
   */
  Snapshot getSnapshot();
}
