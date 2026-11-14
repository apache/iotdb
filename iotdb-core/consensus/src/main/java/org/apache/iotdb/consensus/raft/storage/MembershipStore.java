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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.raft.core.Membership;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@code <groupDir>/membership}: the latest configuration (so it survives log compaction). */
public final class MembershipStore {

  public static final String FILE_NAME = "membership";

  /** A stored configuration and whether its CONFIG entry was known committed. */
  public static final class Stored {
    public final Membership membership;
    public final boolean committed;

    public Stored(Membership membership, boolean committed) {
      this.membership = membership;
      this.committed = committed;
    }
  }

  private final File file;

  public MembershipStore(File groupDir) {
    this.file = new File(groupDir, FILE_NAME);
  }

  public Stored load() throws IOException {
    byte[] p = AtomicFiles.read(file);
    if (p == null) {
      if (file.exists()) {
        throw new IOException("Corrupt Raft membership metadata: " + file);
      }
      return null;
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(p))) {
      long configIndex = in.readLong();
      boolean committed = in.readBoolean();
      List<Integer> voters = readIds(in);
      List<Integer> learners = readIds(in);
      int n = in.readInt();
      if (n < 0 || n > in.available() / 10) {
        throw new IOException("Invalid endpoint count");
      }
      Map<Integer, TEndPoint> eps = new HashMap<>();
      for (int i = 0; i < n; i++) {
        int id = in.readInt();
        String ip = in.readUTF();
        int port = in.readInt();
        eps.put(id, new TEndPoint(ip, port));
      }
      if (in.available() != 0) {
        throw new IOException("Trailing membership metadata");
      }
      return new Stored(new Membership(voters, learners, eps, configIndex), committed);
    }
  }

  public void save(Membership m, boolean committed) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeLong(m.configIndex());
      out.writeBoolean(committed);
      writeIds(out, m.voters());
      writeIds(out, m.learners());
      out.writeInt(m.endpoints().size());
      for (Map.Entry<Integer, TEndPoint> e : m.endpoints().entrySet()) {
        out.writeInt(e.getKey());
        out.writeUTF(e.getValue().getIp());
        out.writeInt(e.getValue().getPort());
      }
    }
    AtomicFiles.write(file, bytes.toByteArray());
  }

  private static void writeIds(DataOutputStream out, Iterable<Integer> ids) throws IOException {
    List<Integer> l = new ArrayList<>();
    for (int i : ids) {
      l.add(i);
    }
    out.writeInt(l.size());
    for (int i : l) {
      out.writeInt(i);
    }
  }

  private static List<Integer> readIds(DataInputStream in) throws IOException {
    int n = in.readInt();
    if (n < 0 || n > in.available() / 4) {
      throw new IOException("Invalid membership count");
    }
    List<Integer> l = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      l.add(in.readInt());
    }
    return l;
  }
}
