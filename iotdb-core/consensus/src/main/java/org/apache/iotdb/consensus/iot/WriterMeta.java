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

package org.apache.iotdb.consensus.iot;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

final class WriterMeta {

  private static final int FORMAT_VERSION = 1;

  private final long writerEpoch;
  private final long lastAllocatedLocalSeq;
  private final long lastAssignedPhysicalTimeMs;

  WriterMeta(long writerEpoch, long lastAllocatedLocalSeq, long lastAssignedPhysicalTimeMs) {
    this.writerEpoch = writerEpoch;
    this.lastAllocatedLocalSeq = lastAllocatedLocalSeq;
    this.lastAssignedPhysicalTimeMs = lastAssignedPhysicalTimeMs;
  }

  long getWriterEpoch() {
    return writerEpoch;
  }

  long getLastAllocatedLocalSeq() {
    return lastAllocatedLocalSeq;
  }

  long getLastAssignedPhysicalTimeMs() {
    return lastAssignedPhysicalTimeMs;
  }

  static Optional<WriterMeta> load(Path path) throws IOException {
    if (!Files.exists(path)) {
      return Optional.empty();
    }
    try (InputStream inputStream = Files.newInputStream(path, StandardOpenOption.READ);
        DataInputStream dataInputStream = new DataInputStream(inputStream)) {
      final int version = dataInputStream.readInt();
      if (version != FORMAT_VERSION) {
        throw new IOException(
            String.format(
                "Unsupported writer meta version %d in %s", version, path.toAbsolutePath()));
      }
      return Optional.of(
          new WriterMeta(
              dataInputStream.readLong(), dataInputStream.readLong(), dataInputStream.readLong()));
    }
  }

  void persist(Path path) throws IOException {
    final Path parent = path.getParent();
    if (parent != null && !Files.exists(parent)) {
      Files.createDirectories(parent);
    }
    final Path tempPath =
        parent == null
            ? Paths.get(path + ".tmp")
            : parent.resolve(path.getFileName().toString() + ".tmp");
    try (OutputStream outputStream =
            Files.newOutputStream(
                tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
      dataOutputStream.writeInt(FORMAT_VERSION);
      dataOutputStream.writeLong(writerEpoch);
      dataOutputStream.writeLong(lastAllocatedLocalSeq);
      dataOutputStream.writeLong(lastAssignedPhysicalTimeMs);
      dataOutputStream.flush();
    }
    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }
}
