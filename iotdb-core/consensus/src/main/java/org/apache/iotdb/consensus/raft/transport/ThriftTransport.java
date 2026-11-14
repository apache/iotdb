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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.raft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.core.action.SendMeta;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.PartitionedMeta;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermReq;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteReq;
import org.apache.iotdb.consensus.raft.core.msg.TimeoutNowReq;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncReq;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesReq;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesRes;
import org.apache.iotdb.consensus.raft.thrift.TRecoverTermRes;
import org.apache.iotdb.consensus.raft.thrift.TRequestVoteRes;
import org.apache.iotdb.consensus.raft.thrift.TTimeoutNowRes;
import org.apache.iotdb.consensus.raft.thrift.TUpdateSyncRes;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * Thrift-backed transport: one async RPC per message; the selector thread only decodes the response
 * and hands an {@code Inbound} (or {@code RpcFailed}) event to the group executor. Entry metadata
 * other than the partitioned (TRaft) one travels through the group's {@link EntryMetaCodec}.
 */
public final class ThriftTransport implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftTransport.class);

  private final TConsensusGroupId gid;
  private final IClientManager<TEndPoint, AsyncRaftServiceClient> clients;
  private final IntFunction<TEndPoint> resolver;
  private final Consumer<Event> sink;
  private final EntryMetaCodec codec;
  private LogFileTransfer logFiles;

  public void setLogFiles(LogFileTransfer logFiles) {
    this.logFiles = logFiles;
  }

  /** A transport with the default codec ({@link PartitionedMeta#CODEC}, the original behaviour). */
  public ThriftTransport(
      TConsensusGroupId gid,
      IClientManager<TEndPoint, AsyncRaftServiceClient> clients,
      IntFunction<TEndPoint> resolver,
      Consumer<Event> sink) {
    this(gid, clients, resolver, sink, PartitionedMeta.CODEC);
  }

  public ThriftTransport(
      TConsensusGroupId gid,
      IClientManager<TEndPoint, AsyncRaftServiceClient> clients,
      IntFunction<TEndPoint> resolver,
      Consumer<Event> sink,
      EntryMetaCodec codec) {
    this.gid = gid;
    this.clients = clients;
    this.resolver = resolver;
    this.sink = sink;
    this.codec = codec;
  }

  private AsyncRaftServiceClient client(int to, long msgId, long[] indexes) {
    TEndPoint ep = resolver.apply(to);
    if (ep == null) {
      LOGGER.debug("{}: unknown endpoint of N{}", gid, to);
      sink.accept(new Event.RpcFailed(to, msgId, indexes));
      return null;
    }
    try {
      return clients.borrowClient(ep);
    } catch (Exception e) {
      LOGGER.debug("{}: cannot connect N{} at {}: {}", gid, to, ep, e.toString());
      sink.accept(new Event.RpcFailed(to, msgId, indexes));
      return null;
    }
  }

  @Override
  public void send(int to, Message msg, long msgId) {
    long[] none = new long[0];
    if (msg instanceof AppendEntriesReq) {
      sendOne(to, (AppendEntriesReq) msg, msgId);
    } else if (msg instanceof RequestVoteReq) {
      AsyncRaftServiceClient c = client(to, msgId, none);
      if (c == null) {
        return;
      }
      try {
        c.requestVote(
            ThriftCodec.encode(gid, (RequestVoteReq) msg),
            new AsyncMethodCallback<TRequestVoteRes>() {
              @Override
              public void onComplete(TRequestVoteRes res) {
                sink.accept(new Event.Inbound(to, ThriftCodec.decode(res), null));
              }

              @Override
              public void onError(Exception e) {
                sink.accept(new Event.RpcFailed(to, msgId, none));
              }
            });
      } catch (Exception e) {
        sink.accept(new Event.RpcFailed(to, msgId, none));
      }
    } else if (msg instanceof TimeoutNowReq) {
      AsyncRaftServiceClient c = client(to, msgId, none);
      if (c == null) {
        return;
      }
      try {
        c.timeoutNow(
            ThriftCodec.encode(gid, (TimeoutNowReq) msg),
            new AsyncMethodCallback<TTimeoutNowRes>() {
              @Override
              public void onComplete(TTimeoutNowRes res) {}

              @Override
              public void onError(Exception e) {
                LOGGER.debug("{}: timeoutNow to N{} failed: {}", gid, to, e.toString());
              }
            });
      } catch (Exception e) {
        LOGGER.debug("{}: timeoutNow to N{} failed: {}", gid, to, e.toString());
      }
    } else if (msg instanceof RecoverTermReq) {
      AsyncRaftServiceClient c = client(to, msgId, none);
      if (c == null) {
        return;
      }
      try {
        c.recoverTerm(
            ThriftCodec.encode(gid, (RecoverTermReq) msg),
            new AsyncMethodCallback<TRecoverTermRes>() {
              @Override
              public void onComplete(TRecoverTermRes res) {
                sink.accept(new Event.Inbound(to, ThriftCodec.decode(res, codec), null));
              }

              @Override
              public void onError(Exception e) {
                sink.accept(new Event.RpcFailed(to, msgId, none));
              }
            });
      } catch (Exception e) {
        sink.accept(new Event.RpcFailed(to, msgId, none));
      }
    } else if (msg instanceof UpdateSyncReq) {
      AsyncRaftServiceClient c = client(to, msgId, none);
      if (c == null) {
        return;
      }
      try {
        c.updateSync(
            ThriftCodec.encode(gid, (UpdateSyncReq) msg),
            new AsyncMethodCallback<TUpdateSyncRes>() {
              @Override
              public void onComplete(TUpdateSyncRes res) {
                sink.accept(new Event.Inbound(to, ThriftCodec.decode(res), null));
              }

              @Override
              public void onError(Exception e) {
                sink.accept(new Event.RpcFailed(to, msgId, none));
              }
            });
      } catch (Exception e) {
        sink.accept(new Event.RpcFailed(to, msgId, none));
      }
    } else {
      throw new IllegalArgumentException("unsupported message " + msg.kind());
    }
  }

  @Override
  public void sendAppend(List<Integer> targets, AppendEntriesReq req, Map<Integer, SendMeta> meta) {
    byte[] file = null;
    if (logFiles != null) {
      try {
        file = logFiles.prepare(req);
      } catch (java.io.IOException error) {
        // A disposable transport file cannot invalidate the retained Raft entries.
        ResearchMetrics.add("lsm-raft.logfile.prepare_failures", 1);
        LOGGER.warn("{}: file preparation failed, sending original entries", gid, error);
      }
    }
    for (int to : targets) {
      SendMeta m = meta == null ? null : meta.get(to);
      AppendEntriesReq r = m == null ? req : req.withEntries(req.entries, m.speculative);
      sendOne(to, r, req.msgId, file);
    }
  }

  private void sendOne(int to, AppendEntriesReq req, long msgId) {
    sendOne(to, req, msgId, null);
  }

  private void sendOne(int to, AppendEntriesReq req, long msgId, byte[] file) {
    long[] indexes = new long[req.entries.size()];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = req.entries.get(i).index;
    }
    AsyncRaftServiceClient c = client(to, msgId, indexes);
    if (c == null) {
      return;
    }
    try {
      ResearchMetrics.add("raft.replication_rpc_attempts", 1);
      if (!req.entries.isEmpty()) {
        ResearchMetrics.add("raft.replication_data_rpcs", 1);
        if (req.entries.size() == 1) {
          ResearchMetrics.add("raft.replication_singleton_rpcs", 1);
        }
      }
      for (LogEntry entry : req.entries) {
        ResearchMetrics.add("raft.replication_payload_bytes_offered", entry.size());
        ResearchMetrics.add("raft.replication_entries_offered", 1);
      }
      // With a file the entries travel inside it: encode only the header, not the structs.
      TAppendEntriesReq encoded =
          ThriftCodec.encode(
              gid,
              file == null
                  ? req
                  : req.withEntries(java.util.Collections.<LogEntry>emptyList(), req.speculative),
              codec);
      if (file != null) {
        encoded.setLogFile(file);
        ResearchMetrics.add("lsm-raft.logfile.files_sent", 1);
        ResearchMetrics.add("lsm-raft.logfile.entries_sent", req.entries.size());
        ResearchMetrics.add("lsm-raft.logfile.file_bytes_sent", file.length);
      } else if (logFiles != null && !req.entries.isEmpty()) {
        ResearchMetrics.add("lsm-raft.logfile.raw_entries_sent", req.entries.size());
      }
      ResearchMetrics.add(
          "raft.replication_wire_record_bytes", LogFileTransfer.payloadBytes(encoded));
      c.appendEntries(
          encoded,
          new AsyncMethodCallback<TAppendEntriesRes>() {
            @Override
            public void onComplete(TAppendEntriesRes res) {
              sink.accept(new Event.Inbound(to, ThriftCodec.decode(res, msgId), null));
            }

            @Override
            public void onError(Exception e) {
              sink.accept(new Event.RpcFailed(to, msgId, indexes));
            }
          });
    } catch (Exception e) {
      sink.accept(new Event.RpcFailed(to, msgId, indexes));
    }
  }

  @Override
  public void close() {}

  /** Shared helper for the empty-entries case. */
  static long[] indexesOf(List<LogEntry> entries) {
    long[] a = new long[entries.size()];
    for (int i = 0; i < a.length; i++) {
      a[i] = entries.get(i).index;
    }
    return a;
  }
}
