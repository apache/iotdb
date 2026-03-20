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

package org.apache.iotdb.db.storageengine.dataregion.consistency;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/** Durable staging journal for follower-side logical repair sessions. */
class LogicalRepairSessionJournal {

  private static final int FORMAT_VERSION = 1;

  private final Path journalDirOverride;
  private final ConcurrentHashMap<String, SessionState> sessions = new ConcurrentHashMap<>();

  LogicalRepairSessionJournal() {
    this(null);
  }

  LogicalRepairSessionJournal(Path journalDir) {
    this.journalDirOverride = journalDir;
  }

  public synchronized void stageBatch(
      String consensusGroupKey,
      long partitionId,
      String repairEpoch,
      String sessionId,
      String treeKind,
      String leafId,
      int seqNo,
      String batchKind,
      ByteBuffer payload)
      throws IOException {
    SessionState sessionState =
        loadOrCreateSession(consensusGroupKey, partitionId, repairEpoch, sessionId);
    if (sessionState.batches.containsKey(seqNo)) {
      return;
    }
    sessionState.batches.put(
        seqNo,
        new StagedBatch(sessionId, treeKind, leafId, seqNo, batchKind, duplicatePayload(payload)));
    persist(sessionState);
  }

  public synchronized List<StagedBatch> loadStagedBatches(
      String consensusGroupKey, long partitionId, String repairEpoch, String sessionId)
      throws IOException {
    SessionState sessionState = loadSession(sessionId);
    if (sessionState == null) {
      return Collections.emptyList();
    }
    validateSession(sessionState, consensusGroupKey, partitionId, repairEpoch, sessionId);
    return new ArrayList<>(sessionState.batches.values());
  }

  public synchronized void completeSession(String sessionId) throws IOException {
    sessions.remove(sessionId);
    Files.deleteIfExists(sessionPath(sessionId));
  }

  private SessionState loadOrCreateSession(
      String consensusGroupKey, long partitionId, String repairEpoch, String sessionId)
      throws IOException {
    SessionState existing = loadSession(sessionId);
    if (existing != null) {
      validateSession(existing, consensusGroupKey, partitionId, repairEpoch, sessionId);
      return existing;
    }

    SessionState created =
        new SessionState(consensusGroupKey, partitionId, repairEpoch, sessionId, new TreeMap<>());
    sessions.put(sessionId, created);
    persist(created);
    return created;
  }

  private SessionState loadSession(String sessionId) throws IOException {
    SessionState cached = sessions.get(sessionId);
    if (cached != null) {
      return cached;
    }

    Path sessionPath = sessionPath(sessionId);
    if (!Files.exists(sessionPath)) {
      return null;
    }

    try (InputStream inputStream = Files.newInputStream(sessionPath, StandardOpenOption.READ)) {
      SessionState loaded = deserialize(inputStream);
      sessions.put(sessionId, loaded);
      return loaded;
    }
  }

  private void validateSession(
      SessionState sessionState,
      String consensusGroupKey,
      long partitionId,
      String repairEpoch,
      String sessionId) {
    if (!Objects.equals(sessionState.consensusGroupKey, consensusGroupKey)
        || sessionState.partitionId != partitionId
        || !Objects.equals(sessionState.repairEpoch, repairEpoch)
        || !Objects.equals(sessionState.sessionId, sessionId)) {
      throw new IllegalStateException(
          "Logical repair session " + sessionId + " conflicts with current request metadata");
    }
  }

  private void persist(SessionState sessionState) throws IOException {
    Path journalDir = getJournalDir();
    Files.createDirectories(journalDir);
    Path sessionPath = sessionPath(sessionState.sessionId);
    Path tmpPath = sessionPath.resolveSibling(sessionPath.getFileName() + ".tmp");

    try (FileOutputStream outputStream = new FileOutputStream(tmpPath.toFile())) {
      serialize(sessionState, outputStream);
      outputStream.getFD().sync();
    }

    try {
      Files.move(
          tmpPath,
          sessionPath,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(tmpPath, sessionPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private Path sessionPath(String sessionId) {
    return getJournalDir().resolve(sessionId + ".session");
  }

  private Path getJournalDir() {
    if (journalDirOverride != null) {
      return journalDirOverride;
    }
    return Paths.get(
        IoTDBDescriptor.getInstance().getConfig().getSystemDir(), "consistency-repair", "sessions");
  }

  private static byte[] duplicatePayload(ByteBuffer payload) {
    if (payload == null) {
      return new byte[0];
    }
    ByteBuffer duplicate = payload.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }

  private static void serialize(SessionState sessionState, OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(FORMAT_VERSION, outputStream);
    ReadWriteIOUtils.write(sessionState.consensusGroupKey, outputStream);
    ReadWriteIOUtils.write(sessionState.partitionId, outputStream);
    ReadWriteIOUtils.write(sessionState.repairEpoch, outputStream);
    ReadWriteIOUtils.write(sessionState.sessionId, outputStream);
    ReadWriteIOUtils.write(sessionState.batches.size(), outputStream);
    for (StagedBatch stagedBatch : sessionState.batches.values()) {
      ReadWriteIOUtils.write(stagedBatch.sessionId, outputStream);
      ReadWriteIOUtils.write(stagedBatch.treeKind, outputStream);
      ReadWriteIOUtils.write(stagedBatch.leafId, outputStream);
      ReadWriteIOUtils.write(stagedBatch.seqNo, outputStream);
      ReadWriteIOUtils.write(stagedBatch.batchKind, outputStream);
      ReadWriteIOUtils.write(stagedBatch.payload.length, outputStream);
      outputStream.write(stagedBatch.payload);
    }
  }

  private static SessionState deserialize(InputStream inputStream) throws IOException {
    int formatVersion = ReadWriteIOUtils.readInt(inputStream);
    if (formatVersion != FORMAT_VERSION) {
      throw new IOException("Unsupported logical repair session format " + formatVersion);
    }
    String consensusGroupKey = ReadWriteIOUtils.readString(inputStream);
    long partitionId = ReadWriteIOUtils.readLong(inputStream);
    String repairEpoch = ReadWriteIOUtils.readString(inputStream);
    String sessionId = ReadWriteIOUtils.readString(inputStream);
    int batchCount = ReadWriteIOUtils.readInt(inputStream);
    Map<Integer, StagedBatch> batches = new TreeMap<>();
    for (int i = 0; i < batchCount; i++) {
      String batchSessionId = ReadWriteIOUtils.readString(inputStream);
      String treeKind = ReadWriteIOUtils.readString(inputStream);
      String leafId = ReadWriteIOUtils.readString(inputStream);
      int seqNo = ReadWriteIOUtils.readInt(inputStream);
      String batchKind = ReadWriteIOUtils.readString(inputStream);
      int payloadSize = ReadWriteIOUtils.readInt(inputStream);
      byte[] payload = new byte[payloadSize];
      int offset = 0;
      while (offset < payloadSize) {
        int read = inputStream.read(payload, offset, payloadSize - offset);
        if (read < 0) {
          throw new IOException("Unexpected end of logical repair session journal");
        }
        offset += read;
      }
      batches.put(
          seqNo, new StagedBatch(batchSessionId, treeKind, leafId, seqNo, batchKind, payload));
    }
    return new SessionState(consensusGroupKey, partitionId, repairEpoch, sessionId, batches);
  }

  static final class StagedBatch {
    private final String sessionId;
    private final String treeKind;
    private final String leafId;
    private final int seqNo;
    private final String batchKind;
    private final byte[] payload;

    private StagedBatch(
        String sessionId,
        String treeKind,
        String leafId,
        int seqNo,
        String batchKind,
        byte[] payload) {
      this.sessionId = sessionId;
      this.treeKind = treeKind;
      this.leafId = leafId;
      this.seqNo = seqNo;
      this.batchKind = batchKind;
      this.payload = payload == null ? new byte[0] : Arrays.copyOf(payload, payload.length);
    }

    String getSessionId() {
      return sessionId;
    }

    String getTreeKind() {
      return treeKind;
    }

    String getLeafId() {
      return leafId;
    }

    int getSeqNo() {
      return seqNo;
    }

    String getBatchKind() {
      return batchKind;
    }

    ByteBuffer duplicatePayload() {
      return ByteBuffer.wrap(Arrays.copyOf(payload, payload.length));
    }
  }

  private static final class SessionState {
    private final String consensusGroupKey;
    private final long partitionId;
    private final String repairEpoch;
    private final String sessionId;
    private final Map<Integer, StagedBatch> batches;

    private SessionState(
        String consensusGroupKey,
        long partitionId,
        String repairEpoch,
        String sessionId,
        Map<Integer, StagedBatch> batches) {
      this.consensusGroupKey = consensusGroupKey;
      this.partitionId = partitionId;
      this.repairEpoch = repairEpoch;
      this.sessionId = sessionId;
      this.batches = batches;
    }
  }
}
