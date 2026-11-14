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

import org.apache.iotdb.consensus.common.research.ResearchIo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/** Small crash-safe files: payload + crc32, written to a temp file, forced, atomically moved. */
public final class AtomicFiles {

  private AtomicFiles() {}

  public static void write(File file, byte[] payload) throws IOException {
    File tmp = new File(file.getPath() + ".tmp");
    ByteBuffer buf = ByteBuffer.allocate(payload.length + 4);
    buf.put(payload);
    CRC32 crc = new CRC32();
    crc.update(payload, 0, payload.length);
    buf.putInt((int) crc.getValue());
    buf.flip();
    try (FileChannel ch =
        FileChannel.open(
            tmp.toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      while (buf.hasRemaining()) {
        ResearchIo.write(ch, buf, ResearchIo.COMMON_METADATA);
      }
      ResearchIo.force(ch, true, ResearchIo.COMMON_METADATA);
    }
    Files.move(
        tmp.toPath(),
        file.toPath(),
        StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING);
    forceDirectory(file.getAbsoluteFile().getParentFile());
  }

  /** Create directory ancestors and force their parent links before acknowledging durability. */
  public static void createDirectories(File directory) throws IOException {
    java.util.List<File> missing = new java.util.ArrayList<>();
    File ancestor = directory.getAbsoluteFile();
    while (ancestor != null && !ancestor.exists()) {
      missing.add(ancestor);
      ancestor = ancestor.getParentFile();
    }
    Files.createDirectories(directory.toPath());
    for (int i = missing.size() - 1; i >= 0; i--) {
      forceDirectory(missing.get(i).getParentFile());
    }
  }

  /** Persist newly created or renamed directory entries before acknowledging durability. */
  public static void forceDirectory(File directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory.toPath(), StandardOpenOption.READ)) {
      ResearchIo.force(channel, true, ResearchIo.COMMON_DIRECTORY);
    }
  }

  /** Persist a self-contained snapshot before publishing its manifest or acknowledging install. */
  public static void forceTree(File root) throws IOException {
    try (java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(root.toPath())) {
      java.util.List<java.nio.file.Path> all = paths.collect(java.util.stream.Collectors.toList());
      for (java.nio.file.Path path : all) {
        if (Files.isRegularFile(path)) {
          try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ResearchIo.force(channel, true, ResearchIo.COMMON_SNAPSHOT);
          }
        }
      }
      all.sort(java.util.Comparator.reverseOrder());
      for (java.nio.file.Path path : all) {
        if (Files.isDirectory(path)) {
          forceDirectory(path.toFile());
        }
      }
    }
  }

  /** The payload, or null if the file is missing or its crc does not match. */
  public static byte[] read(File file) throws IOException {
    if (!file.exists()) {
      return null;
    }
    byte[] all = ResearchIo.readAllBytes(file.toPath(), ResearchIo.COMMON_METADATA);
    if (all.length < 4) {
      return null;
    }
    int n = all.length - 4;
    CRC32 crc = new CRC32();
    crc.update(all, 0, n);
    int stored = ByteBuffer.wrap(all, n, 4).getInt();
    if (stored != (int) crc.getValue()) {
      return null;
    }
    byte[] payload = new byte[n];
    System.arraycopy(all, 0, payload, 0, n);
    return payload;
  }
}
