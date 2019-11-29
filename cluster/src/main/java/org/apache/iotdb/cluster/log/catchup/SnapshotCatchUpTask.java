/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.catchup;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.SnapshotCatchUpHandler;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SnapshotCatchUpTask first sends the snapshot to the stale node then sends the logs to the node.
 */
public class SnapshotCatchUpTask extends LogCatchUpTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotCatchUpTask.class);

  private Snapshot snapshot;

  public SnapshotCatchUpTask(List<Log> logs, Snapshot snapshot, Node node, RaftMember raftMember) {
    super(logs, node, raftMember);
    this.snapshot = snapshot;
  }

  private boolean doSnapshotCatchUp() {
    AsyncClient client = raftMember.connectNode(node);
    if (client == null) {
      return false;
    }

    SendSnapshotRequest request = new SendSnapshotRequest();
    if (raftMember instanceof DataGroupMember) {
      request.setHeader(((DataGroupMember) raftMember).getHeader());
    }
    request.setSnapshotBytes(snapshot.serialize());

    AtomicBoolean succeed = new AtomicBoolean(false);
    SnapshotCatchUpHandler handler = new SnapshotCatchUpHandler(succeed, node, snapshot);
    synchronized (raftMember.getTerm()) {
      // make sure this node is still a leader
      if (raftMember.getCharacter() != NodeCharacter.LEADER) {
        logger.debug("Leadership is lost when doing a catch-up to {}, aborting", node);
        return false;
      }
    }

    synchronized (succeed) {
      try {
        client.sendSnapshot(request, handler);
        raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
        succeed.wait(RaftServer.CONNECTION_TIME_OUT_MS);
      } catch (TException e) {
        logger.error("Cannot send snapshot {} to {}", snapshot, node);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Catch up {} is interrupted:", node, e);
      }
    }

    return succeed.get();
  }

  @Override
  public void run() {
    try {
      if (doSnapshotCatchUp()) {
        doLogCatchUp();
      }
    } catch (Exception e) {
      logger.error("Catch up {} errored", node, e);
    }
    logger.debug("Catch up {} finished", node);
    // the next catch up is enabled
    raftMember.getLastCatchUpResponseTime().remove(node);
  }
}
