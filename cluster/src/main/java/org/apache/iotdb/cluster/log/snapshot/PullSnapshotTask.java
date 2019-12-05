/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.snapshot;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.handlers.caller.PullSnapshotHandler;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullSnapshotTask<T extends Snapshot> implements Callable<Map<Integer,
    T>> {

  private static final Logger logger = LoggerFactory.getLogger(PullSnapshotTask.class);

  private List<Integer> sockets;
  // the new member created by a node addition
  private DataGroupMember newMember;
  // the nodes the may hold the target socket
  private List<Node> oldMembers;
  // the header of the old members
  private Node header;

  private PullSnapshotRequest request;
  private SnapshotFactory snapshotFactory;

  public PullSnapshotTask(Node header, List<Integer> sockets,
      DataGroupMember member, List<Node> oldMembers, SnapshotFactory snapshotFactory) {
    this.header = header;
    this.sockets = sockets;
    this.newMember = member;
    this.oldMembers = oldMembers;
    this.snapshotFactory = snapshotFactory;
  }

  private boolean pullSnapshot(AtomicReference<Map<Integer, T>> snapshotRef, int nodeIndex)
      throws InterruptedException, TException {
    Node node = oldMembers.get(nodeIndex);
    TSDataService.AsyncClient client =
        (TSDataService.AsyncClient) newMember.connectNode(node);
    if (client == null) {
      // network is bad, wait and retry
      Thread.sleep(ClusterConstant.CONNECTION_TIME_OUT_MS);
    } else {
      synchronized (snapshotRef) {
        client.pullSnapshot(request, new PullSnapshotHandler<>(snapshotRef,
            node, sockets, snapshotFactory));
        snapshotRef.wait(ClusterConstant.CONNECTION_TIME_OUT_MS);
      }
      Map<Integer, T> result = snapshotRef.get();
      if (result != null) {
        if (logger.isInfoEnabled()) {
          logger.info("Received a snapshot {} from {}", snapshotRef.get(),
              oldMembers.get(nodeIndex));
        }
        for (Entry<Integer, T> entry : result.entrySet()) {
          newMember.applySnapshot(entry.getValue(), entry.getKey());
        }
        return true;
      } else {
        Thread.sleep(ClusterConstant.PULL_SNAPSHOT_RETRY_INTERVAL);
      }
    }
    return false;
  }

  @Override
  public Map<Integer, T> call() {
    request = new PullSnapshotRequest();
    request.setHeader(header);
    request.setRequiredSockets(sockets);
    AtomicReference<Map<Integer, T>> snapshotRef = new AtomicReference<>();
    boolean finished = false;
    int nodeIndex = -1;
    while (!finished) {
      try {
        // sequentially pick up a node that may have this socket
        nodeIndex = (nodeIndex + 1) % oldMembers.size();
        finished = pullSnapshot(snapshotRef, nodeIndex);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when pulling socket {}", sockets, e);
        finished = true;
      } catch (TException e) {
        logger.debug("Cannot pull socket {} from {}, retry", sockets, header, e);
      }
    }
    return snapshotRef.get();
  }
}
