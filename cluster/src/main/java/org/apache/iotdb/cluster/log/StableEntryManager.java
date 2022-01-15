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

import org.apache.iotdb.cluster.log.manage.serializable.LogManagerMeta;

import java.io.IOException;
import java.util.List;

public interface StableEntryManager {

  List<Log> getAllEntriesAfterAppliedIndex();

  List<Log> getAllEntriesAfterCommittedIndex();

  void append(List<Log> entries, long maxHaveAppliedCommitIndex) throws IOException;

  void flushLogBuffer();

  void forceFlushLogBuffer();

  void removeCompactedEntries(long index);

  void setHardStateAndFlush(HardState state);

  HardState getHardState();

  LogManagerMeta getMeta();

  /**
   * @param startIndex (inclusive) the log start index
   * @param endIndex (inclusive) the log end index
   * @return the raft log which index between [startIndex, endIndex] or empty if not found
   */
  List<Log> getLogs(long startIndex, long endIndex);

  void close();

  /**
   * clear all logs, this method mainly used for after a follower accept a snapshot, all the logs
   * should be cleaned
   */
  void clearAllLogs(long commitIndex);
}
