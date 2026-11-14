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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@code <logDir>/meta}: first index still in the log (after compaction), the term of the entry
 * before it and the protocol summary at that boundary (TRaft partition summary).
 */
public final class LogMeta {

  public static final String FILE_NAME = "meta";

  private final File file;
  private long firstIndex;
  private long snapshotTerm;
  private byte[] profileSummary = new byte[0];

  public LogMeta(File logDir) {
    this.file = new File(logDir, FILE_NAME);
  }

  public LogMeta load() throws IOException {
    byte[] p = AtomicFiles.read(file);
    if (p == null) {
      if (file.exists()) {
        throw new IOException("Corrupt Raft log metadata: " + file);
      }
      return this;
    }
    if (p.length < 20) {
      throw new IOException("Truncated Raft log metadata");
    }
    ByteBuffer b = ByteBuffer.wrap(p);
    long first = b.getLong();
    long term = b.getLong();
    int n = b.getInt();
    if (first < 0 || n < 0 || n != b.remaining()) {
      throw new IOException("Invalid Raft log metadata length or boundary");
    }
    firstIndex = first;
    snapshotTerm = term;
    profileSummary = new byte[n];
    b.get(profileSummary);
    return this;
  }

  public void save(long firstIndex, long snapshotTerm, byte[] profileSummary) throws IOException {
    byte[] ext = profileSummary == null ? new byte[0] : profileSummary;
    ByteBuffer b = ByteBuffer.allocate(20 + ext.length);
    b.putLong(firstIndex).putLong(snapshotTerm).putInt(ext.length).put(ext);
    AtomicFiles.write(file, b.array());
    this.firstIndex = firstIndex;
    this.snapshotTerm = snapshotTerm;
    this.profileSummary = ext;
  }

  public long firstIndex() {
    return firstIndex;
  }

  public long snapshotTerm() {
    return snapshotTerm;
  }

  public byte[] profileSummary() {
    return profileSummary;
  }
}
