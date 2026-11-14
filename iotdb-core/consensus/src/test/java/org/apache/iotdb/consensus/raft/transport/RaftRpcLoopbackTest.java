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

package org.apache.iotdb.consensus.raft.transport;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.raft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.client.RaftClientPool;
import org.apache.iotdb.consensus.raft.client.SyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.VoteKey;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteReq;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteRes;
import org.apache.iotdb.consensus.raft.service.RaftRPCService;
import org.apache.iotdb.consensus.raft.service.RaftRPCServiceProcessor;
import org.apache.iotdb.consensus.raft.service.RaftRpcDispatcher;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusReq;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusRes;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Real Thrift loopback: a {@link RaftRPCService} on a free localhost port answers with a stub
 * dispatcher; {@link ThriftTransport} sends AppendEntries / RequestVote through the async client
 * pool and the responses arrive as kernel events; unreachable peers produce RpcFailed.
 */
public class RaftRpcLoopbackTest {

  private static final ConsensusGroupId GID =
      ConsensusGroupId.Factory.create(TConsensusGroupType.DataRegion.getValue(), 1);
  private static final TConsensusGroupId TGID = GID.convertToTConsensusGroupId();

  private RaftRPCService service;
  private IClientManager<TEndPoint, AsyncRaftServiceClient> asyncClients;
  private IClientManager<TEndPoint, SyncRaftServiceClient> syncClients;
  private int port;
  private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();
  private final List<Message> received = Collections.synchronizedList(new ArrayList<>());

  private static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  @Before
  public void setUp() throws Exception {
    port = freePort();
    RaftConfig cfg = RaftConfig.forTests();
    RaftRpcDispatcher stub =
        new RaftRpcDispatcher() {
          @Override
          public CompletableFuture<Message> handle(ConsensusGroupId gid, int from, Message msg) {
            if (!gid.equals(GID)) {
              return null;
            }
            received.add(msg);
            if (msg instanceof AppendEntriesReq) {
              AppendEntriesReq ae = (AppendEntriesReq) msg;
              List<EntryAck> acks = new ArrayList<>();
              for (LogEntry e : ae.entries) {
                acks.add(EntryAck.ack(e.index, e.term));
              }
              return CompletableFuture.completedFuture(
                  new AppendEntriesRes(ae.term, acks, 3, 3, -1, AppendEntriesRes.UNSET, ae.msgId));
            }
            if (msg instanceof RequestVoteReq) {
              return CompletableFuture.completedFuture(new RequestVoteRes(msg.term, true, false));
            }
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public TGroupStatusRes groupStatus(ConsensusGroupId gid) {
            return new TGroupStatusRes(9, 1, 2, 5, 5, 0, 6, 6);
          }
        };
    service =
        new RaftRPCService(
            new TEndPoint("127.0.0.1", port),
            cfg.rpc,
            new RaftRPCServiceProcessor(stub, cfg.rpc.rpcTimeoutMs));
    service.start();
    asyncClients = RaftClientPool.newAsyncManager(cfg.rpc);
    syncClients = RaftClientPool.newSyncManager(cfg.rpc);
  }

  @After
  public void tearDown() {
    if (asyncClients != null) {
      asyncClients.close();
    }
    if (syncClients != null) {
      syncClients.close();
    }
    if (service != null) {
      service.stop();
    }
  }

  private ThriftTransport transport() {
    return new ThriftTransport(
        TGID,
        asyncClients,
        id ->
            id == 2
                ? new TEndPoint("127.0.0.1", port)
                : id == 3 ? new TEndPoint("127.0.0.1", 1) : null,
        events::add);
  }

  @Test
  public void appendEntriesAndVoteRoundTrip() throws Exception {
    assertTrue(service.isServing());
    ThriftTransport t = transport();
    LogEntry e0 =
        new LogEntry(
            0,
            2,
            EntryType.DATA,
            EntryMeta.EMPTY,
            ByteBuffer.wrap("A".getBytes(StandardCharsets.UTF_8)));
    LogEntry e1 =
        new LogEntry(
            1,
            2,
            EntryType.DATA,
            EntryMeta.EMPTY,
            ByteBuffer.wrap("B".getBytes(StandardCharsets.UTF_8)));
    AppendEntriesReq req =
        new AppendEntriesReq(2, 1, -1, 0, Arrays.asList(e0, e1), 1, 2, false, false, 42, 0);
    t.sendAppend(Collections.singletonList(2), req, null);
    Event ev = events.poll(10, TimeUnit.SECONDS);
    assertNotNull(ev);
    assertTrue(ev instanceof Event.Inbound);
    Event.Inbound in = (Event.Inbound) ev;
    assertEquals(2, in.from);
    AppendEntriesRes res = (AppendEntriesRes) in.msg;
    assertEquals(2, res.acks.size());
    assertEquals(1, res.acks.get(1).index);
    assertEquals(42, res.msgId);
    assertEquals(3, res.appliedIndex);
    AppendEntriesReq seen = (AppendEntriesReq) received.get(0);
    assertEquals("B", seen.entries.get(1).payloadAsString());

    t.send(2, new RequestVoteReq(3, 1, VoteKey.raft(2, 1), false), 43);
    Event ev2 = events.poll(10, TimeUnit.SECONDS);
    assertNotNull(ev2);
    RequestVoteRes vr = (RequestVoteRes) ((Event.Inbound) ev2).msg;
    assertTrue(vr.granted);
    assertEquals(3, vr.term);
  }

  @Test
  public void unreachablePeerYieldsRpcFailed() throws Exception {
    ThriftTransport t = transport();
    LogEntry e0 = new LogEntry(0, 2, EntryType.DATA, EntryMeta.EMPTY, ByteBuffer.allocate(0));
    t.sendAppend(
        Collections.singletonList(3),
        new AppendEntriesReq(2, 1, -1, 0, Collections.singletonList(e0), -1, 0, false, false, 7, 0),
        null);
    Event ev = events.poll(15, TimeUnit.SECONDS);
    assertNotNull(ev);
    assertTrue(ev.toString(), ev instanceof Event.RpcFailed);
    assertEquals(3, ((Event.RpcFailed) ev).to);
    assertEquals(0, ((Event.RpcFailed) ev).indexes[0]);
    // unknown node id: failed immediately
    t.send(9, new RequestVoteReq(1, 1, VoteKey.raft(0, -1), false), 8);
    Event ev2 = events.poll(5, TimeUnit.SECONDS);
    assertTrue(ev2 instanceof Event.RpcFailed);
  }

  @Test
  public void syncClientServesStatus() throws Exception {
    try (SyncRaftServiceClient c = syncClients.borrowClient(new TEndPoint("127.0.0.1", port))) {
      TGroupStatusRes st = c.getGroupStatus(new TGroupStatusReq(TGID));
      assertEquals(9, st.getTerm());
      assertEquals(6, st.getContiguousEnd());
    }
  }
}
