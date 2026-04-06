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

package org.apache.iotdb.consensus.traft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Disk-backed log store used by TRaft.
 *
 * <p>The store enforces contiguous Raft log indexes and relies on callers to truncate conflicting
 * suffixes before appending replacement entries.
 */
class TRaftLogStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(TRaftLogStore.class);
  private static final String LOG_FILE_NAME = "traft.log";

  private final File logFile;
  private final NavigableMap<Long, TRaftLogEntry> logEntries = new TreeMap<>();

  TRaftLogStore(String storageDir) throws IOException {
    this.logFile = new File(storageDir, LOG_FILE_NAME);
    File parent = logFile.getParentFile();
    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      throw new IOException(String.format("Failed to create TRaft dir %s", parent));
    }
    if (!logFile.exists() && !logFile.createNewFile()) {
      throw new IOException(String.format("Failed to create TRaft log file %s", logFile));
    }
    loadAll();
  }

  synchronized void append(TRaftLogEntry entry) throws IOException {
    TRaftLogEntry existing = logEntries.get(entry.getLogIndex());
    if (existing != null) {
      if (isSameEntry(existing, entry)) {
        return;
      }
      throw new IOException(
          String.format("Conflicting TRaft log entry at index %s", entry.getLogIndex()));
    }
    if (!logEntries.isEmpty() && entry.getLogIndex() != logEntries.lastKey() + 1) {
      throw new IOException(
          String.format(
              "Non-contiguous TRaft log append. expected=%s actual=%s",
              logEntries.lastKey() + 1,
              entry.getLogIndex()));
    }
    logEntries.put(entry.getLogIndex(), entry.copy());
    persistAll();
  }

  synchronized void appendAll(List<TRaftLogEntry> entries) throws IOException {
    for (TRaftLogEntry entry : entries) {
      TRaftLogEntry existing = logEntries.get(entry.getLogIndex());
      if (existing != null && !isSameEntry(existing, entry)) {
        throw new IOException(
            String.format("Conflicting TRaft log entry at index %s", entry.getLogIndex()));
      }
      logEntries.put(entry.getLogIndex(), entry.copy());
    }
    persistAll();
  }

  synchronized void truncateSuffix(long startIndexInclusive) throws IOException {
    logEntries.tailMap(startIndexInclusive, true).clear();
    persistAll();
  }

  synchronized void compactPrefix(long lastIncludedIndex) throws IOException {
    logEntries.headMap(lastIncludedIndex, true).clear();
    persistAll();
  }

  synchronized void clear() throws IOException {
    logEntries.clear();
    persistAll();
  }

  synchronized TRaftLogEntry getByIndex(long index) {
    TRaftLogEntry entry = logEntries.get(index);
    return entry == null ? null : entry.copy();
  }

  synchronized long getFirstIndex() {
    return logEntries.isEmpty() ? -1 : logEntries.firstKey();
  }

  synchronized long getLastIndex() {
    return logEntries.isEmpty() ? -1 : logEntries.lastKey();
  }

  synchronized long getTerm(long index) {
    TRaftLogEntry entry = logEntries.get(index);
    return entry == null ? -1 : entry.getLogTerm();
  }

  synchronized TRaftLogEntry getLastEntry() {
    return logEntries.isEmpty() ? null : logEntries.lastEntry().getValue().copy();
  }

  synchronized List<TRaftLogEntry> getEntriesFrom(long startIndexInclusive, int maxEntries) {
    if (maxEntries <= 0) {
      return Collections.emptyList();
    }
    List<TRaftLogEntry> result = new ArrayList<>();
    for (TRaftLogEntry entry : logEntries.tailMap(startIndexInclusive, true).values()) {
      result.add(entry.copy());
      if (result.size() >= maxEntries) {
        break;
      }
    }
    return Collections.unmodifiableList(result);
  }

  synchronized List<TRaftLogEntry> getAllEntries() {
    List<TRaftLogEntry> result = new ArrayList<>(logEntries.size());
    for (TRaftLogEntry entry : logEntries.values()) {
      result.add(entry.copy());
    }
    return Collections.unmodifiableList(result);
  }

  private void loadAll() throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(logFile.toPath(), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        TRaftLogEntry entry = deserialize(line);
        logEntries.put(entry.getLogIndex(), entry);
      }
    }
  }

  private void persistAll() throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(logFile.toPath(), StandardCharsets.UTF_8)) {
      for (TRaftLogEntry entry : logEntries.values()) {
        writer.write(serialize(entry));
        writer.newLine();
      }
    }
  }

  private boolean isSameEntry(TRaftLogEntry left, TRaftLogEntry right) {
    return left.getLogIndex() == right.getLogIndex()
        && left.getLogTerm() == right.getLogTerm()
        && left.getEntryType() == right.getEntryType()
        && left.getTimestamp() == right.getTimestamp()
        && left.getPartitionIndex() == right.getPartitionIndex()
        && left.getInterPartitionIndex() == right.getInterPartitionIndex()
        && left.getLastPartitionCount() == right.getLastPartitionCount()
        && java.util.Arrays.equals(left.getData(), right.getData());
  }

  private String serialize(TRaftLogEntry entry) {
    return entry.getLogIndex()
        + ","
        + entry.getLogTerm()
        + ","
        + entry.getEntryType().name()
        + ","
        + entry.getTimestamp()
        + ","
        + entry.getPartitionIndex()
        + ","
        + entry.getInterPartitionIndex()
        + ","
        + entry.getLastPartitionCount()
        + ","
        + Base64.getEncoder().encodeToString(entry.getData());
  }

  private TRaftLogEntry deserialize(String line) {
    String[] fields = line.split(",", 8);
    if (fields.length != 8) {
      throw new IllegalArgumentException("Invalid TRaft log line: " + line);
    }
    try {
      long logIndex = Long.parseLong(fields[0]);
      long logTerm = Long.parseLong(fields[1]);
      TRaftEntryType entryType = TRaftEntryType.valueOf(fields[2]);
      long timestamp = Long.parseLong(fields[3]);
      long partitionIndex = Long.parseLong(fields[4]);
      long interPartitionIndex = Long.parseLong(fields[5]);
      long lastPartitionCount = Long.parseLong(fields[6]);
      byte[] data = Base64.getDecoder().decode(fields[7]);
      return new TRaftLogEntry(
          entryType,
          timestamp,
          partitionIndex,
          logIndex,
          logTerm,
          interPartitionIndex,
          lastPartitionCount,
          data);
    } catch (Exception e) {
      LOGGER.error("Failed to parse TRaft log line {}", line, e);
      throw e;
    }
  }
}
