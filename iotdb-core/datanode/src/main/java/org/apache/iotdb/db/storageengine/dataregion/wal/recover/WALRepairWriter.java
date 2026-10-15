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

package org.apache.iotdb.db.storageengine.dataregion.wal.recover;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/** Check whether the wal file is broken and repair it. */
public class WALRepairWriter {
  private final File logFile;

  public WALRepairWriter(File logFile) {
    this.logFile = logFile;
  }

  /**
   * Repairs a WAL from the readable prefix. Returns {@code false} when the file has no recoverable
   * entry and is moved aside with a {@code .broken} suffix.
   */
  public boolean repair(WALMetaData metaData) throws IOException {
    if (isEmptyOrHeaderOnly()) {
      return true;
    }

    WALFileVersion version = WALFileVersion.getVersion(logFile);
    if (hasReadableMetadata()) {
      return true;
    }

    // The caller has already scanned the readable entries and supplied their rebuilt metadata.
    if (metaData.getBuffersSize().isEmpty()) {
      quarantine();
      return false;
    }
    // A channel offset may include a partially read entry in the same compressed segment. Rebuild
    // complete entries in a temporary file instead of truncating at a read-ahead offset. Publish
    // only after every entry and the new footer have been written and forced successfully.
    Path repaired =
        Files.createTempFile(logFile.toPath().toAbsolutePath().getParent(), "wal-repair-", ".tmp");
    try {
      try (WALByteBufReader reader = new WALByteBufReader(logFile, metaData)) {
        if (version == WALFileVersion.V1) {
          // V1 entries are raw bytes, without the segment framing emitted by modern WALWriter.
          try (FileChannel output = FileChannel.open(repaired, StandardOpenOption.WRITE)) {
            while (reader.hasNext()) {
              writeFully(output, reader.next());
            }
            ByteBuffer footer =
                ByteBuffer.allocate(
                    1
                        + metaData.serializedSize(version)
                        + Integer.BYTES
                        + version.getVersionBytes().length);
            footer.put(WALEntryType.WAL_FILE_INFO_END_MARKER.getCode());
            metaData.serialize(footer, version);
            footer.putInt(metaData.serializedSize(version)).put(version.getVersionBytes()).flip();
            writeFully(output, footer);
            output.force(true);
          }
        } else {
          try (WALWriter writer = new WALWriter(repaired.toFile(), version)) {
            while (reader.hasNext()) {
              ByteBuffer entry = reader.next();
              entry.position(entry.limit());
              writer.write(entry, false);
            }
            writer.updateMetaData(metaData);
          }
        }
      }
      try {
        Files.move(
            repaired,
            logFile.toPath(),
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(repaired, logFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(repaired);
    }
    return true;
  }

  private static void writeFully(FileChannel output, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      output.write(buffer);
    }
  }

  private boolean isEmptyOrHeaderOnly() throws IOException {
    try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ)) {
      return WALFileVersion.isEmptyOrHeaderOnly(channel);
    }
  }

  private boolean hasReadableMetadata() {
    try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ)) {
      WALMetaData.readFromWALFileWithoutRecovery(logFile, channel);
      return true;
    } catch (IOException | RuntimeException e) {
      return false;
    }
  }

  /** Moves an unrecoverable WAL aside without replacing an earlier quarantined file. */
  public void quarantine() throws IOException {
    int suffix = 0;
    while (true) {
      File target = new File(logFile.getPath() + ".broken" + (suffix == 0 ? "" : "." + suffix));
      try {
        // ATOMIC_MOVE may overwrite an existing target on some providers. A no-replace move
        // preserves evidence even when another reader chooses the same quarantine name.
        Files.move(logFile.toPath(), target.toPath());
        return;
      } catch (FileAlreadyExistsException e) {
        suffix++;
      }
    }
  }
}
