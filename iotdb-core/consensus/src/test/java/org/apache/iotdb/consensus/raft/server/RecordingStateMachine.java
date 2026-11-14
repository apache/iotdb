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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** Records applied payloads as strings; remembers whether they came from a remote leader. */
public class RecordingStateMachine implements IStateMachine, IStateMachine.EventApi {

  /** A request carrying a UTF-8 string. */
  public static class StringRequest implements IConsensusRequest {
    public final String value;
    public boolean remote;

    public StringRequest(String value) {
      this.value = value;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void markAsGeneratedByRemoteConsensusLeader() {
      remote = true;
    }
  }

  public final List<String> applied = new CopyOnWriteArrayList<>();
  public final AtomicInteger remoteApplied = new AtomicInteger();
  public volatile int leaderId = -1;
  public volatile boolean leaderReady;

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public TSStatus write(IConsensusRequest request) {
    StringRequest r = (StringRequest) request;
    applied.add(r.value);
    if (r.remote) {
      remoteApplied.incrementAndGet();
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public IConsensusRequest deserializeRequest(IConsensusRequest request) {
    if (request instanceof StringRequest) {
      return request;
    }
    ByteBuffer b = ((ByteBufferConsensusRequest) request).serializeToByteBuffer();
    byte[] a = new byte[b.remaining()];
    b.duplicate().get(a);
    return new StringRequest(new String(a, StandardCharsets.UTF_8));
  }

  /** What a read returns: the applied values at read time. */
  public static final class AppliedView implements DataSet {
    public final List<String> applied;

    AppliedView(List<String> applied) {
      this.applied = applied;
    }
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    return new AppliedView(new ArrayList<>(applied));
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    try {
      java.nio.file.Files.write(
          new File(snapshotDir, "applied.txt").toPath(),
          String.join("\n", applied).getBytes(StandardCharsets.UTF_8));
      snapshotsTaken.incrementAndGet();
      return true;
    } catch (java.io.IOException e) {
      return false;
    }
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    try {
      byte[] bytes =
          java.nio.file.Files.readAllBytes(new File(latestSnapshotRootDir, "applied.txt").toPath());
      String text = new String(bytes, StandardCharsets.UTF_8);
      applied.clear();
      if (!text.isEmpty()) {
        applied.addAll(java.util.Arrays.asList(text.split("\n")));
      }
      snapshotsLoaded.incrementAndGet();
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public final AtomicInteger snapshotsTaken = new AtomicInteger();
  public final AtomicInteger snapshotsLoaded = new AtomicInteger();

  @Override
  public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
    leaderId = newLeaderId;
  }

  @Override
  public void notifyLeaderReady() {
    leaderReady = true;
  }

  @Override
  public void notifyNotLeader() {
    leaderReady = false;
  }

  public List<String> snapshotApplied() {
    return new ArrayList<>(applied);
  }
}
