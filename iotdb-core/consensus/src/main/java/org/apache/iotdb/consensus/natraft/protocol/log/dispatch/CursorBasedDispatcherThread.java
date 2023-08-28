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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch;

import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class CursorBasedDispatcherThread extends DispatcherThread {

  private static final Logger logger = LoggerFactory.getLogger(CursorBasedDispatcherThread.class);
  private final AtomicLong cursor;

  protected CursorBasedDispatcherThread(
      LogDispatcher logDispatcher, Peer receiver, AtomicLong cursor, DispatcherGroup group) {
    super(logDispatcher, receiver, group);
    this.cursor = cursor;
  }

  @Override
  protected boolean fetchLogs() throws InterruptedException {
    if (!logDispatcher.getMember().isLeader()) {
      synchronized (cursor) {
        cursor.wait(1000);
      }
      return false;
    }

    if (group.isDelayed()) {
      if (logDispatcher.getMember().getLogManager().getLastLogIndex() - cursor.get()
              < logDispatcher.maxBatchSize
          && System.nanoTime() - lastDispatchTime < 1_000_000_000L) {
        // the follower is being delayed, if there is not enough requests, and it has
        // dispatched recently, wait for a while to get a larger batch
        Thread.sleep(100);
        return false;
      }
    }

    synchronized (cursor) {
      long cursorIndex = cursor.get();
      List<Entry> entries =
          logDispatcher.getMember().getLogManager().getEntries(cursorIndex, Long.MAX_VALUE);
      if (entries.size() > logDispatcher.maxBatchSize) {
        entries = entries.subList(0, logDispatcher.maxBatchSize);
      }
      List<VotingEntry> votingEntries =
          entries.stream().map(this::buildVotingEntry).collect(Collectors.toList());

      if (!votingEntries.isEmpty()) {
        currBatch.addAll(votingEntries);
        Entry lastEntry = votingEntries.get(votingEntries.size() - 1).getEntry();
        cursor.set(lastEntry.getCurrLogIndex() + 1);
      } else {
        if (group.getLogDispatcher().getMember().isLeader()) {
          cursor.wait(1000);
        } else {
          cursor.wait(5000);
        }
        return false;
      }
    }
    return true;
  }

  protected VotingEntry buildVotingEntry(Entry entry) {
    VotingEntry votingEntry = entry.getVotingEntry();
    if (votingEntry != null) {
      return votingEntry;
    }

    synchronized (entry) {
      votingEntry = entry.getVotingEntry();
      if (votingEntry != null) {
        return votingEntry;
      }

      votingEntry = logDispatcher.getMember().buildVotingLog(entry);
      entry.setVotingEntry(votingEntry);
    }
    return votingEntry;
  }
}
