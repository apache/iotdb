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

package org.apache.iotdb.db.storageengine.dataregion.objectgc;

import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.utils.writelog.LogWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Per-DataRegion rolling journal under {@code {dataRegionSysDir}/object-gc/}: {@code gc.log.{seq}}
 * plus an atomic {@code checkpoint} of (seq, offset).
 */
public class ObjectGcJournal implements AutoCloseable {

  public static final String JOURNAL_DIR_NAME = "object-gc";
  public static final String MAGIC = "OBJGC001";
  public static final int MAGIC_BYTES = MAGIC.length();
  public static final int LENGTH_BYTES = Integer.BYTES;
  public static final int CRC_BYTES = Long.BYTES;
  public static final long DEFAULT_ROLL_SIZE_BYTES = 64L * 1024 * 1024;

  private final File journalDir;
  private final long rollSizeBytes;
  private final Object lock = new Object();

  private long currentSeq;
  private LogWriter writer;
  private File currentLogFile;
  private long checkpointSeq;
  private long checkpointOffset;

  public ObjectGcJournal(File dataRegionSysDir) throws IOException {
    this(dataRegionSysDir, DEFAULT_ROLL_SIZE_BYTES);
  }

  public ObjectGcJournal(File dataRegionSysDir, long rollSizeBytes) throws IOException {
    this.journalDir = new File(dataRegionSysDir, JOURNAL_DIR_NAME);
    this.rollSizeBytes = rollSizeBytes;
    if (!journalDir.exists() && !journalDir.mkdirs() && !journalDir.exists()) {
      throw new IOException(
          String.format(
              StorageEngineMessages.EXCEPTION_CANNOT_CREATE_OBJECT_GC_DIR_ARG_055D7364,
              journalDir));
    }
    loadCheckpoint();
    openWriterForAppend();
  }

  public File getJournalDir() {
    return journalDir;
  }

  public long getCheckpointSeq() {
    return checkpointSeq;
  }

  public long getCheckpointOffset() {
    return checkpointOffset;
  }

  public long[] readCheckpoint() {
    synchronized (lock) {
      return new long[] {checkpointSeq, checkpointOffset};
    }
  }

  public void append(ObjectGcRecord record) throws IOException {
    ByteBuffer payload = record.serialize();
    synchronized (lock) {
      writer.write(payload);
      writer.force();
      if (currentLogFile.length() >= rollSizeBytes) {
        roll();
      }
    }
  }

  public void checkpoint(long seq, long offset) throws IOException {
    synchronized (lock) {
      File tmp = new File(journalDir, "checkpoint.tmp");
      File ckpt = checkpointFile();
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.putLong(seq);
      buffer.putLong(offset);
      buffer.flip();
      try (FileChannel channel =
          FileChannel.open(
              tmp.toPath(),
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE)) {
        channel.write(buffer);
        channel.force(true);
      }
      Files.move(
          tmp.toPath(),
          ckpt.toPath(),
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      this.checkpointSeq = seq;
      this.checkpointOffset = offset;
      deleteObsoleteLogs(seq);
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (lock) {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    }
  }

  File logFile(long seq) {
    return new File(journalDir, "gc.log." + seq);
  }

  File checkpointFile() {
    return new File(journalDir, "checkpoint");
  }

  private void loadCheckpoint() throws IOException {
    File ckpt = checkpointFile();
    if (!ckpt.exists()) {
      checkpointSeq = 0;
      checkpointOffset = MAGIC_BYTES;
      return;
    }
    ByteBuffer buffer = ByteBuffer.allocate(16);
    try (FileChannel channel = FileChannel.open(ckpt.toPath(), StandardOpenOption.READ)) {
      int read = channel.read(buffer);
      if (read < 16) {
        checkpointSeq = 0;
        checkpointOffset = MAGIC_BYTES;
        return;
      }
    }
    buffer.flip();
    checkpointSeq = buffer.getLong();
    checkpointOffset = buffer.getLong();
  }

  private void openWriterForAppend() throws IOException {
    currentSeq = Math.max(checkpointSeq, maxExistingSeq());
    currentLogFile = logFile(currentSeq);
    boolean newFile = !currentLogFile.exists() || currentLogFile.length() == 0;
    if (newFile) {
      try (FileChannel channel =
          FileChannel.open(
              currentLogFile.toPath(),
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.TRUNCATE_EXISTING)) {
        channel.write(ByteBuffer.wrap(MAGIC.getBytes(StandardCharsets.US_ASCII)));
        channel.force(true);
      }
    }
    writer = new LogWriter(currentLogFile, false);
  }

  private void roll() throws IOException {
    writer.close();
    currentSeq++;
    currentLogFile = logFile(currentSeq);
    try (FileChannel channel =
        FileChannel.open(
            currentLogFile.toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      channel.write(ByteBuffer.wrap(MAGIC.getBytes(StandardCharsets.US_ASCII)));
      channel.force(true);
    }
    writer = new LogWriter(currentLogFile, false);
  }

  private long maxExistingSeq() {
    File[] logs = journalDir.listFiles((dir, name) -> name.startsWith("gc.log."));
    if (logs == null || logs.length == 0) {
      return 0;
    }
    return Arrays.stream(logs)
        .map(file -> file.getName().substring("gc.log.".length()))
        .mapToLong(Long::parseLong)
        .max()
        .orElse(0);
  }

  private void deleteObsoleteLogs(long ckptSeq) {
    File[] logs = journalDir.listFiles((dir, name) -> name.startsWith("gc.log."));
    if (logs == null) {
      return;
    }
    Arrays.sort(
        logs,
        Comparator.comparingLong(f -> Long.parseLong(f.getName().substring("gc.log.".length()))));
    for (File log : logs) {
      long seq = Long.parseLong(log.getName().substring("gc.log.".length()));
      if (seq >= ckptSeq) {
        break;
      }
      try {
        Files.deleteIfExists(log.toPath());
      } catch (IOException ignored) {
        // best-effort recycle of already-checkpointed logs
      }
    }
  }
}
