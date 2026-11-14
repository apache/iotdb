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

package org.apache.iotdb.consensus.raft.core.log;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.raft.core.Membership;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Payload of a CONFIG entry: the complete new configuration. */
public final class MembershipCodec {

  private MembershipCodec() {}

  public static ByteBuffer encode(Membership m) {
    int size = 8 + 4 + 4 + 4;
    for (int id : m.voters()) {
      size += 4;
    }
    for (int id : m.learners()) {
      size += 4;
    }
    List<byte[]> ips = new ArrayList<>();
    for (Map.Entry<Integer, TEndPoint> e : m.endpoints().entrySet()) {
      byte[] ip = e.getValue().getIp().getBytes(StandardCharsets.UTF_8);
      ips.add(ip);
      size += 4 + 4 + ip.length + 4;
    }
    ByteBuffer b = ByteBuffer.allocate(size);
    b.putLong(m.configIndex());
    b.putInt(m.voters().size());
    for (int id : m.voters()) {
      b.putInt(id);
    }
    b.putInt(m.learners().size());
    for (int id : m.learners()) {
      b.putInt(id);
    }
    b.putInt(m.endpoints().size());
    int i = 0;
    for (Map.Entry<Integer, TEndPoint> e : m.endpoints().entrySet()) {
      byte[] ip = ips.get(i++);
      b.putInt(e.getKey()).putInt(ip.length).put(ip).putInt(e.getValue().getPort());
    }
    b.flip();
    return b;
  }

  public static Membership decode(ByteBuffer in) {
    ByteBuffer b = in.duplicate();
    long configIndex = b.getLong();
    int nv = b.getInt();
    List<Integer> voters = new ArrayList<>(nv);
    for (int i = 0; i < nv; i++) {
      voters.add(b.getInt());
    }
    int nl = b.getInt();
    List<Integer> learners = new ArrayList<>(nl);
    for (int i = 0; i < nl; i++) {
      learners.add(b.getInt());
    }
    int ne = b.getInt();
    Map<Integer, TEndPoint> eps = new HashMap<>();
    for (int i = 0; i < ne; i++) {
      int id = b.getInt();
      byte[] ip = new byte[b.getInt()];
      b.get(ip);
      eps.put(id, new TEndPoint(new String(ip, StandardCharsets.UTF_8), b.getInt()));
    }
    return new Membership(voters, learners, eps, configIndex);
  }
}
