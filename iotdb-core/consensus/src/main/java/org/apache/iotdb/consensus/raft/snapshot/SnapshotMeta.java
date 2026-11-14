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

package org.apache.iotdb.consensus.raft.snapshot;

import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.core.log.MembershipCodec;
import org.apache.iotdb.consensus.raft.storage.AtomicFiles;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/** The {@code manifest} of a snapshot directory: last included index/term, membership, ext. */
public final class SnapshotMeta {

  public static final String MANIFEST = "manifest";

  public final long index;
  public final long term;
  public final Membership membership;
  public final byte[] ext;
  public final File dir;

  public SnapshotMeta(long index, long term, Membership membership, byte[] ext, File dir) {
    this.index = index;
    this.term = term;
    this.membership = membership;
    this.ext = ext == null ? new byte[0] : ext;
    this.dir = dir;
  }

  public String id() {
    return term + "_" + index;
  }

  public void save() throws IOException {
    ByteBuffer m = MembershipCodec.encode(membership);
    ByteBuffer b = ByteBuffer.allocate(8 + 8 + 4 + m.remaining() + 4 + ext.length);
    b.putLong(index).putLong(term).putInt(m.remaining()).put(m).putInt(ext.length).put(ext);
    AtomicFiles.write(new File(dir, MANIFEST), b.array());
  }

  /** Null when the directory holds no valid manifest. */
  public static SnapshotMeta load(File dir) throws IOException {
    byte[] p = AtomicFiles.read(new File(dir, MANIFEST));
    if (p == null || p.length < 24) {
      return null;
    }
    ByteBuffer b = ByteBuffer.wrap(p);
    long index = b.getLong();
    long term = b.getLong();
    int ml = b.getInt();
    ByteBuffer mb = b.duplicate();
    mb.limit(b.position() + ml);
    Membership m = MembershipCodec.decode(mb);
    b.position(b.position() + ml);
    byte[] ext = new byte[b.getInt()];
    b.get(ext);
    return new SnapshotMeta(index, term, m, ext, dir);
  }

  @Override
  public String toString() {
    return "Snapshot{" + id() + "," + membership + "}";
  }
}
