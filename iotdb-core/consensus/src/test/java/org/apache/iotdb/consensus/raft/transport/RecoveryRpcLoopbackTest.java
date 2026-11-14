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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.raft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.client.RaftClientPool;
import org.apache.iotdb.consensus.raft.client.SyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.AppendExt;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermReq;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermRes;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncReq;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncRes;
import org.apache.iotdb.consensus.raft.server.WriteForwarder;
import org.apache.iotdb.consensus.raft.service.RaftRPCService;
import org.apache.iotdb.consensus.raft.service.RaftRPCServiceProcessor;
import org.apache.iotdb.consensus.raft.service.RaftRpcDispatcher;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteReq;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteRes;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusRes;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Real Thrift loopback for the ParallelRaft plumbing: RecoverTerm / UpdateSync through {@link
 * ThriftTransport} and the RPC processor, the group's entry-metadata codec on both sides, the
 * AppendEntries extension and response fields, and the forwarded request extension.
 */
public class RecoveryRpcLoopbackTest {

  private static final ConsensusGroupId GID =
      ConsensusGroupId.Factory.create(TConsensusGroupType.DataRegion.getValue(), 3);
  private static final TConsensusGroupId TGID = GID.convertToTConsensusGroupId();

  private RaftRPCService service;
  private IClientManager<TEndPoint, AsyncRaftServiceClient> asyncClients;
  private IClientManager<TEndPoint, SyncRaftServiceClient> syncClients;
  private int port;
  private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();
  private final List<Message> received = Collections.synchronizedList(new ArrayList<>());
  private final List<TForwardWriteReq> forwarded = Collections.synchronizedList(new ArrayList<>());

  private static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  private static LogEntry dated(long index, long term, String data, long date) {
    return new LogEntry(
        index,
        term,
        EntryType.DATA,
        new DatedTestMeta(date, (int) index),
        ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
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
                  new AppendEntriesRes(
                      ae.term,
                      acks,
                      AppendEntriesRes.UNSET,
                      4,
                      -1,
                      AppendEntriesRes.UNSET,
                      ae.msgId,
                      2,
                      1));
            }
            if (msg instanceof RecoverTermReq) {
              RecoverTermReq rt = (RecoverTermReq) msg;
              return CompletableFuture.completedFuture(
                  new RecoverTermRes(
                      rt.term,
                      true,
                      rt.syncTerm,
                      12,
                      1,
                      rt.term - 1,
                      13,
                      Arrays.asList(
                          dated(rt.fromIndex, rt.syncTerm, "x", rt.syncTerm),
                          dated(rt.fromIndex + 2, rt.syncTerm, "z", rt.term - 1)),
                      new long[] {rt.fromIndex},
                      true,
                      rt.fromIndex + 3));
            }
            if (msg instanceof UpdateSyncReq) {
              UpdateSyncReq us = (UpdateSyncReq) msg;
              return CompletableFuture.completedFuture(
                  new UpdateSyncRes(us.term, true, us.newSync, 13));
            }
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public TGroupStatusRes groupStatus(ConsensusGroupId gid) {
            return new TGroupStatusRes(9, 1, 2, 5, 5, 0, 6, 6);
          }

          @Override
          public EntryMetaCodec metaCodec(ConsensusGroupId gid) {
            return DatedTestMeta.CODEC;
          }

          @Override
          public TForwardWriteRes forwardWrite(TForwardWriteReq req) {
            forwarded.add(req);
            return new TForwardWriteRes(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
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
        events::add,
        DatedTestMeta.CODEC);
  }

  private Message nextInbound(int from) throws InterruptedException {
    Event ev = events.poll(10, TimeUnit.SECONDS);
    assertNotNull(ev);
    assertTrue(ev.toString(), ev instanceof Event.Inbound);
    assertEquals(from, ((Event.Inbound) ev).from);
    return ((Event.Inbound) ev).msg;
  }

  @Test
  public void appendEntriesCarryProfileMetaExtAndSyncFields() throws Exception {
    ThriftTransport t = transport();
    AppendEntriesReq req =
        new AppendEntriesReq(
                5,
                1,
                -1,
                0,
                Arrays.asList(dated(7, 4, "a", 5), dated(9, 4, "b", 4)),
                6,
                4,
                false,
                false,
                51,
                0)
            .withExt(AppendExt.push(10, 4));
    t.sendAppend(Collections.singletonList(2), req, null);
    AppendEntriesRes res = (AppendEntriesRes) nextInbound(2);
    assertEquals(2, res.acks.size());
    assertEquals(2, res.syncNumber);
    assertEquals(1, res.logFirstIndex);
    assertEquals(4, res.contiguousEnd);
    assertEquals(51, res.msgId);
    AppendEntriesReq seen = (AppendEntriesReq) received.get(0);
    assertEquals("PUSH", seen.kind());
    assertEquals(req.ext, seen.ext);
    DatedTestMeta m = (DatedTestMeta) seen.entries.get(0).meta; // decoded with the group's codec
    assertEquals(5, m.date);
    assertEquals(7, m.tag);
    assertEquals("b", seen.entries.get(1).payloadAsString());
  }

  @Test
  public void recoverTermAndUpdateSyncRoundTrip() throws Exception {
    ThriftTransport t = transport();
    t.send(2, new RecoverTermReq(6, 1, 4, 10, 128, 1 << 20), 0);
    RecoverTermRes rtr = (RecoverTermRes) nextInbound(2);
    RecoverTermReq seen = (RecoverTermReq) received.get(0);
    assertEquals(6, seen.term);
    assertEquals(4, seen.syncTerm);
    assertEquals(10, seen.fromIndex);
    assertEquals(128, seen.maxEntries);
    assertEquals(1 << 20, seen.maxBytes);
    assertEquals(6, rtr.term);
    assertTrue(rtr.granted);
    assertEquals(4, rtr.syncNumber);
    assertEquals(12, rtr.contiguousEnd);
    assertEquals(1, rtr.logFirstIndex);
    assertEquals(5, rtr.promiseBallot);
    assertEquals(13, rtr.promiseEnd);
    assertEquals(2, rtr.entries.size());
    assertEquals(5, ((DatedTestMeta) rtr.entries.get(1).meta).date); // encoded with the group codec
    assertEquals("z", rtr.entries.get(1).payloadAsString());
    assertArrayEquals(new long[] {10}, rtr.committedIndexes);
    assertTrue(rtr.more);
    assertEquals(13, rtr.nextIndex);

    t.send(2, new UpdateSyncReq(6, 1, 6), 0);
    UpdateSyncRes usr = (UpdateSyncRes) nextInbound(2);
    assertTrue(usr.granted);
    assertEquals(6, usr.syncNumber);
    assertEquals(13, usr.contiguousEnd);
    assertEquals(6, ((UpdateSyncReq) received.get(1)).newSync);
  }

  @Test
  public void unreachablePeerFailsRecoveryRpcs() throws Exception {
    ThriftTransport t = transport();
    t.send(3, new RecoverTermReq(6, 1, 4, 10, 128, 1 << 20), 17);
    Event ev = events.poll(15, TimeUnit.SECONDS);
    assertNotNull(ev);
    assertTrue(ev.toString(), ev instanceof Event.RpcFailed);
    assertEquals(3, ((Event.RpcFailed) ev).to);
    assertEquals(17, ((Event.RpcFailed) ev).msgId);
    assertEquals(0, ((Event.RpcFailed) ev).indexes.length);
    t.send(9, new UpdateSyncReq(6, 1, 6), 18); // unknown node id: failed at once
    Event ev2 = events.poll(5, TimeUnit.SECONDS);
    assertTrue(ev2 instanceof Event.RpcFailed);
    assertEquals(9, ((Event.RpcFailed) ev2).to);
    assertEquals(0, ((Event.RpcFailed) ev2).indexes.length);
  }

  @Test
  public void forwardedWriteCarriesTheRequestExt() {
    WriteForwarder f = new WriteForwarder(syncClients);
    TEndPoint leader = new TEndPoint("127.0.0.1", port);
    TSStatus s =
        f.forward(
            GID,
            leader,
            ByteBuffer.wrap(new byte[] {1}),
            RequestMeta.timed(3, 4).withExt(new byte[] {7, 8}));
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), s.getCode());
    f.forward(GID, leader, ByteBuffer.wrap(new byte[] {2}), RequestMeta.OPAQUE);
    assertEquals(2, forwarded.size());
    TForwardWriteReq keyed = forwarded.get(0);
    assertArrayEquals(new byte[] {7, 8}, keyed.getRequestExt());
    assertEquals(3, keyed.getTimestamp());
    assertEquals(4, keyed.getTimestampMax());
    TForwardWriteReq plain = forwarded.get(1);
    assertFalse(plain.isSetRequestExt()); // Raft / TRaft: unchanged request
    assertTrue(plain.isBarrier());
  }
}
