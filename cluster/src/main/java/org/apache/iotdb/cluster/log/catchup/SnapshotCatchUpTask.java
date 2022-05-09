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

package org.apache.iotdb.cluster.log.catchup;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.handlers.caller.SnapshotCatchUpHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.ClientUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SnapshotCatchUpTask first sends the snapshot to the stale node then sends the logs to the node.
 */
public class SnapshotCatchUpTask extends LogCatchUpTask implements Callable<Boolean> {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotCatchUpTask.class);

  // sending a snapshot may take longer than normal communications
  private static final long SEND_SNAPSHOT_WAIT_MS =
      ClusterDescriptor.getInstance().getConfig().getCatchUpTimeoutMS();
  private Snapshot snapshot;

  SnapshotCatchUpTask(
      List<Log> logs, Snapshot snapshot, Node node, int raftId, RaftMember raftMember) {
    super(logs, node, raftId, raftMember);
    this.snapshot = snapshot;
  }

  private void doSnapshotCatchUp() throws TException, InterruptedException, LeaderUnknownException {
    SendSnapshotRequest request = new SendSnapshotRequest();
    if (raftMember.getHeader() != null) {
      request.setHeader(raftMember.getHeader());
    }
    logger.info("Start to send snapshot to {}", node);
    ByteBuffer data = snapshot.serialize();
    if (logger.isInfoEnabled()) {
      logger.info("Do snapshot catch up with size {}", data.array().length);
    }
    request.setSnapshotBytes(data);

    synchronized (raftMember.getTerm()) {
      // make sure this node is still a leader
      if (raftMember.getCharacter() != NodeCharacter.LEADER) {
        throw new LeaderUnknownException(raftMember.getAllNodes());
      }
    }

    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      abort = !sendSnapshotAsync(request);
    } else {
      abort = !sendSnapshotSync(request);
    }
  }

  @SuppressWarnings("java:S2274") // enable timeout
  private boolean sendSnapshotAsync(SendSnapshotRequest request)
      throws TException, InterruptedException {
    AtomicBoolean succeed = new AtomicBoolean(false);
    SnapshotCatchUpHandler handler = new SnapshotCatchUpHandler(succeed, node, snapshot);
    AsyncClient client = raftMember.getAsyncClient(node);
    if (client == null) {
      logger.info("{}: client null for node {}", raftMember.getThisNode(), node);
      abort = true;
      return false;
    }

    logger.info(
        "{}: the snapshot request size={}",
        raftMember.getName(),
        request.getSnapshotBytes().length);
    synchronized (succeed) {
      client.sendSnapshot(request, handler);
      raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
      succeed.wait(SEND_SNAPSHOT_WAIT_MS);
    }
    if (logger.isInfoEnabled()) {
      logger.info("send snapshot to node {} success {}", raftMember.getThisNode(), succeed.get());
    }
    return succeed.get();
  }

  private boolean sendSnapshotSync(SendSnapshotRequest request) throws TException {
    logger.info(
        "{}: sending a snapshot request size={} to {}",
        raftMember.getName(),
        request.getSnapshotBytes().length,
        node);
    Client client = raftMember.getSyncClient(node);
    if (client == null) {
      return false;
    }
    try {
      try {
        client.sendSnapshot(request);
        logger.info("{}: snapshot is sent to {}", raftMember.getName(), node);
        return true;
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        throw e;
      }
    } finally {
      ClientUtils.putBackSyncClient(client);
    }
  }

  @Override
  public Boolean call() throws InterruptedException, TException, LeaderUnknownException {
    doSnapshotCatchUp();
    if (abort) {
      logger.warn("{}: Snapshot catch up {} failed", raftMember.getName(), node);
      raftMember.getLastCatchUpResponseTime().remove(node);
      return false;
    }
    logger.info(
        "{}: Snapshot catch up {} finished, begin to catch up log", raftMember.getName(), node);
    doLogCatchUp();
    if (!abort) {
      logger.info("{}: Catch up {} finished", raftMember.getName(), node);
    } else {
      logger.warn("{}: Log catch up {} failed", raftMember.getName(), node);
    }
    // the next catch up is enabled
    raftMember.getLastCatchUpResponseTime().remove(node);
    return !abort;
  }
}
