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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.raft.client.SyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteReq;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteRes;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/** Forwards a write from a non-leader to the leader over the synchronous client pool. */
public final class WriteForwarder {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteForwarder.class);

  private final IClientManager<TEndPoint, SyncRaftServiceClient> clients;

  public WriteForwarder(IClientManager<TEndPoint, SyncRaftServiceClient> clients) {
    this.clients = clients;
  }

  public TSStatus forward(
      ConsensusGroupId gid, TEndPoint leader, ByteBuffer payload, RequestMeta meta) {
    TForwardWriteReq req = request(gid.convertToTConsensusGroupId(), payload, meta);
    try (SyncRaftServiceClient c = clients.borrowClient(leader)) {
      TForwardWriteRes res = c.forwardWrite(req);
      TSStatus status = res.getStatus();
      status.setRedirectNode(leader);
      return status;
    } catch (Exception e) {
      LOGGER.warn("{}: forwarding a write to leader {} failed: {}", gid, leader, e.toString());
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT, "forward to leader failed: " + e);
    }
  }

  /**
   * The forwarded write: the payload with the metadata the forwarding node extracted (timestamps or
   * the barrier flag, and the protocol extension only if there is one, so that Raft / TRaft
   * requests keep their wire format).
   */
  public static TForwardWriteReq request(
      TConsensusGroupId gid, ByteBuffer payload, RequestMeta meta) {
    TForwardWriteReq req = new TForwardWriteReq(gid, payload);
    if (!meta.isBarrier()) {
      req.setTimestamp(meta.ts);
      req.setTimestampMax(meta.tsMax);
    } else {
      req.setBarrier(true);
    }
    if (meta.ext != null) {
      req.setRequestExt(meta.ext); // ParallelRaft conflict keys; unset for Raft / TRaft
    }
    return req;
  }
}
