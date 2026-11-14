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

package org.apache.iotdb.consensus.raft.storage;

import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;

import java.io.IOException;
import java.util.List;

/**
 * Durable log with holes. {@link #write} and {@link #deleteFrom} are buffered; {@link #sync} makes
 * everything written before it durable and may run while the log-writer thread keeps writing.
 * Written by the log-writer thread, read by anyone.
 */
public interface LogStore extends AutoCloseable {

  /** Append (index may be non-contiguous or below the current max: hole filling / overwrite). */
  void write(LogEntry e) throws IOException;

  /**
   * Append several entries in list order, as {@link #write} each; a store may use one write call.
   */
  default void writeAll(List<LogEntry> entries) throws IOException {
    for (LogEntry e : entries) {
      write(e);
    }
  }

  /** Logically remove every index >= from (a TRUNCATE record; O(1)). */
  void deleteFrom(long from) throws IOException;

  /** Everything written or deleted so far is durable when this returns. */
  void sync() throws IOException;

  /** The entry, or null if absent. Served from the cache when possible. */
  LogEntry read(long index) throws IOException;

  /** Drop whole segments that only hold indexes below {@code index}. */
  void purgeBefore(long index) throws IOException;

  /** Replay the on-disk records into a fresh index (only durable records survive). */
  LogIndex recover() throws IOException;

  LogMeta meta();

  /** Number of cache misses served from disk (replication-cost metric). */
  long loadCount();

  @Override
  void close() throws IOException;
}
