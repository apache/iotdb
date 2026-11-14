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

package org.apache.iotdb.consensus.common.research;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Common accounting at the actual file-I/O boundary. Calls retain their synchronous durability
 * semantics. Read/write bytes describe Java file I/O, which may be satisfied by the OS page cache;
 * they are not measurements of physical device traffic.
 */
public final class ResearchIo {
  public static final String RAFT_LOG = "raft.log";
  public static final String TRAFT_LOG = "traft.log";
  public static final String COMMON_METADATA = "common.metadata";
  public static final String COMMON_DIRECTORY = "common.directory";
  public static final String COMMON_SNAPSHOT = "common.snapshot";

  static {
    for (String scope :
        new String[] {RAFT_LOG, TRAFT_LOG, COMMON_METADATA, COMMON_DIRECTORY, COMMON_SNAPSHOT}) {
      for (String metric :
          new String[] {
            "_force_count",
            "_force_nanos",
            "_force_failure_count",
            "_read_count",
            "_read_bytes",
            "_write_count",
            "_written_bytes"
          }) {
        ResearchMetrics.add(scope + metric, 0);
      }
    }
  }

  /**
   * Benchmark only: extra latency per log force, from {@code -Dresearch.force_delay_us} (default 0,
   * none). fsync on a development laptop returns in tens of microseconds, because macOS fsync does
   * not flush the drive cache; this models a server disk, and models it per node, unlike the one
   * disk the benchmark's three in-JVM nodes share.
   */
  private static final long FORCE_DELAY_NANOS =
      TimeUnit.MICROSECONDS.toNanos(Long.getLong("research.force_delay_us", 0));

  private ResearchIo() {}

  /**
   * Count an attempted force, including its elapsed time and failure if it throws. A count alone is
   * not proof that durability succeeded. The caller receives the original exception.
   */
  public static long force(FileChannel channel, boolean metadata, String scope) throws IOException {
    ResearchMetrics.add(scope + "_force_failure_count", 0);
    ResearchMetrics.add(scope + "_force_count", 1);
    long start = System.nanoTime();
    boolean succeeded = false;
    long elapsed;
    try {
      channel.force(metadata);
      if (FORCE_DELAY_NANOS > 0 && (RAFT_LOG.equals(scope) || TRAFT_LOG.equals(scope))) {
        LockSupport.parkNanos(FORCE_DELAY_NANOS);
      }
      succeeded = true;
    } finally {
      elapsed = System.nanoTime() - start;
      ResearchMetrics.add(scope + "_force_nanos", elapsed);
      if (!succeeded) {
        ResearchMetrics.add(scope + "_force_failure_count", 1);
      }
    }
    return elapsed;
  }

  /** Count channel calls separately from bytes successfully transferred by those calls. */
  public static int write(FileChannel channel, ByteBuffer source, String scope) throws IOException {
    ResearchMetrics.add(scope + "_write_count", 1);
    int written = channel.write(source);
    if (written > 0) {
      ResearchMetrics.add(scope + "_written_bytes", written);
    }
    return written;
  }

  /**
   * Positional reads include journal recovery, validation and cache misses. Cache hits do no I/O.
   */
  public static int read(FileChannel channel, ByteBuffer target, long position, String scope)
      throws IOException {
    ResearchMetrics.add(scope + "_read_count", 1);
    int read = channel.read(target, position);
    if (read > 0) {
      ResearchMetrics.add(scope + "_read_bytes", read);
    }
    return read;
  }

  /** Read a stable file while using the same accounting as incremental journal reads. */
  public static byte[] readAllBytes(Path path, String scope) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      long length = channel.size();
      if (length > Integer.MAX_VALUE) {
        throw new IOException("File is too large for an in-memory recovery read: " + path);
      }
      ByteBuffer bytes = ByteBuffer.allocate((int) length);
      long position = 0;
      while (bytes.hasRemaining()) {
        int count = read(channel, bytes, position, scope);
        if (count < 0) {
          throw new IOException("File shortened during recovery read: " + path);
        }
        position += count;
      }
      return bytes.array();
    }
  }
}
