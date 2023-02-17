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
package org.apache.iotdb.db.wal.buffer;

import java.util.concurrent.TimeUnit;

/**
 * This class serializes and flushes {@link WALEntry}. If search is enabled, the order of search
 * index should be protected by the upper layer, and the value should start from 1.
 */
public interface IWALBuffer extends AutoCloseable {
  /**
   * Write WALEntry into wal buffer.
   *
   * @param walEntry info will be written into wal buffer
   */
  void write(WALEntry walEntry);

  /** Get current log version id */
  long getCurrentWALFileVersion();

  /** Get current wal file's size */
  long getCurrentWALFileSize();

  /** Get current search index */
  long getCurrentSearchIndex();

  @Override
  void close();

  /** Wait for next flush operation done */
  void waitForFlush() throws InterruptedException;

  /** Wait for next flush operation done */
  boolean waitForFlush(long time, TimeUnit unit) throws InterruptedException;

  /** Return true when all wal entries all consumed and flushed */
  boolean isAllWALEntriesConsumed();
}
