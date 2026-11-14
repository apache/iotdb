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
 * {@code <groupDir>/term_vote}: {@code term i64 | votedFor i32 | [len i32 | profileMeta]} + crc32;
 * temp file + fsync + atomic move. The bracketed section is written only when the profile meta is
 * non-empty, so Raft / TRaft keep the original 12-byte payload (readable by older versions), and a
 * 12-byte payload loads with an empty profile meta.
 */
public final class FileMetaStore implements MetaStore {

  public static final String FILE_NAME = "term_vote";

  private static final int FIXED = 8 + 4;

  private final File file;

  public FileMetaStore(File groupDir) {
    this.file = new File(groupDir, FILE_NAME);
  }

  @Override
  public PersistentMeta load() throws IOException {
    byte[] p = AtomicFiles.read(file);
    if (p == null) {
      if (file.exists()) {
        throw new IOException("Corrupt Raft term/vote metadata: " + file);
      }
      return PersistentMeta.INITIAL;
    }
    if (p.length < FIXED) {
      throw new IOException("Truncated Raft term/vote metadata");
    }
    ByteBuffer b = ByteBuffer.wrap(p);
    long term = b.getLong();
    int votedFor = b.getInt();
    if (p.length == FIXED) {
      return new PersistentMeta(term, votedFor); // original format
    }
    if (p.length < FIXED + 4) {
      throw new IOException("Truncated Raft term/vote profile length");
    }
    int len = b.getInt();
    if (len < 0 || len != b.remaining()) {
      throw new IOException("Invalid Raft term/vote profile length");
    }
    byte[] profileMeta = new byte[len];
    b.get(profileMeta);
    return new PersistentMeta(term, votedFor, profileMeta);
  }

  @Override
  public void persist(long term, int votedFor, byte[] profileMeta) throws IOException {
    int len = profileMeta == null ? 0 : profileMeta.length;
    ByteBuffer b = ByteBuffer.allocate(len == 0 ? FIXED : FIXED + 4 + len);
    b.putLong(term).putInt(votedFor);
    if (len > 0) {
      b.putInt(len).put(profileMeta);
    }
    AtomicFiles.write(file, b.array());
  }
}
