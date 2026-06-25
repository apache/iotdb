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

package org.apache.iotdb.consensus.iot.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class SnapshotFragmentReader {

  public static final int DEFAULT_FILE_FRAGMENT_SIZE = 10 * 1024 * 1024;
  private final String snapshotId;
  private final String filePath;
  private final SeekableByteChannel fileChannel;
  private final long fileSize;
  private final ByteBuffer buf;
  private long totalReadSize;
  private SnapshotFragment cachedSnapshotFragment;

  /**
   * The {@code buf} is supplied (and owned) by the caller so a single 10MB buffer can be reused
   * across every file of a snapshot transmission. Allocating a fresh 10MB buffer per file is
   * extremely wasteful when a snapshot contains hundreds of thousands of tiny files, multiplying GC
   * pressure and allocation cost. The buffer is fully reset via {@link ByteBuffer#clear()} on each
   * {@link #hasNext()} call, and each fragment is serialized synchronously before the next read, so
   * sharing it across files (and across readers) is safe.
   */
  public SnapshotFragmentReader(String snapshotId, Path path, ByteBuffer buf) throws IOException {
    this.snapshotId = snapshotId;
    this.filePath = path.toAbsolutePath().toString();
    this.fileSize = Files.size(path);
    this.fileChannel = Files.newByteChannel(path);
    this.buf = buf;
  }

  public boolean hasNext() throws IOException {
    buf.clear();
    int readSize = fileChannel.read(buf);
    buf.flip();
    if (readSize > 0) {
      cachedSnapshotFragment =
          new SnapshotFragment(snapshotId, filePath, fileSize, totalReadSize, readSize, buf);
      totalReadSize += readSize;
      return true;
    }
    return false;
  }

  public SnapshotFragment next() {
    return cachedSnapshotFragment;
  }

  public void close() throws IOException {
    if (fileChannel != null) {
      fileChannel.close();
    }
  }

  public long getTotalReadSize() {
    return totalReadSize;
  }
}
