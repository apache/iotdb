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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class TRaftLogStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(TRaftLogStore.class);
  private static final String LOG_FILE_NAME = "traft.log";

  private final File logFile;
  private final Map<Long, TRaftLogEntry> logEntries = new LinkedHashMap<>();

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
    if (logEntries.containsKey(entry.getLogIndex())) {
      return;
    }
    try (BufferedWriter writer =
        Files.newBufferedWriter(
            logFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
      writer.write(serialize(entry));
      writer.newLine();
    }
    logEntries.put(entry.getLogIndex(), entry.copy());
  }

  synchronized TRaftLogEntry getByIndex(long index) {
    TRaftLogEntry entry = logEntries.get(index);
    return entry == null ? null : entry.copy();
  }

  synchronized boolean contains(long index) {
    return logEntries.containsKey(index);
  }

  synchronized TRaftLogEntry getLastEntry() {
    if (logEntries.isEmpty()) {
      return null;
    }
    TRaftLogEntry last = null;
    for (TRaftLogEntry entry : logEntries.values()) {
      last = entry;
    }
    return last == null ? null : last.copy();
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

  private String serialize(TRaftLogEntry entry) {
    return entry.getLogIndex()
        + ","
        + entry.getLogTerm()
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
    String[] fields = line.split(",", 7);
    if (fields.length != 7) {
      throw new IllegalArgumentException("Invalid TRaft log line: " + line);
    }
    try {
      long logIndex = Long.parseLong(fields[0]);
      long logTerm = Long.parseLong(fields[1]);
      long timestamp = Long.parseLong(fields[2]);
      long partitionIndex = Long.parseLong(fields[3]);
      long interPartitionIndex = Long.parseLong(fields[4]);
      long lastPartitionCount = Long.parseLong(fields[5]);
      byte[] data = Base64.getDecoder().decode(fields[6]);
      return new TRaftLogEntry(
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
