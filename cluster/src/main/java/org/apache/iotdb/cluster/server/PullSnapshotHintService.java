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

package org.apache.iotdb.cluster.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.snapshot.PullSnapshotTaskDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullSnapshotHintService {

  private static final Logger logger = LoggerFactory.getLogger(PullSnapshotHintService.class);

  private DataGroupMember member;
  private ScheduledExecutorService service;
  private ConcurrentLinkedDeque<PullSnapshotHint> hints;

  public PullSnapshotHintService(DataGroupMember member) {
    this.member = member;
    this.hints = new ConcurrentLinkedDeque<>();
  }

  public void start() {
    this.service = Executors.newScheduledThreadPool(1);
    this.service.scheduleAtFixedRate(this::sendHints, 0, 1, TimeUnit.MINUTES);
  }

  public void stop() {
    if (service == null) {
      return;
    }

    service.shutdown();
    try {
      service.awaitTermination(3, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("{}: PullSnapshotHintService exiting interrupted", member.getName());
    }
    service = null;
  }

  public void registerHint(PullSnapshotTaskDescriptor descriptor) {
    PullSnapshotHint hint = new PullSnapshotHint();
    hint.receivers = new ArrayList<>(descriptor.getPreviousHolders());
    hint.header = descriptor.getPreviousHolders().getHeader();
    hint.slots = descriptor.getSlots();
    hints.add(hint);
  }

  private void sendHints() {
    for (Iterator<PullSnapshotHint> iterator = hints.iterator(); iterator.hasNext(); ) {
      PullSnapshotHint hint = iterator.next();
      for (Iterator<Node> iter = hint.receivers.iterator(); iter.hasNext(); ) {
        Node receiver = iter.next();
        try {
          boolean result = sendHint(receiver, hint);
          if (result) {
            iter.remove();
          }
        } catch (TException e) {
          logger.warn("Cannot send pull snapshot hint to {}", receiver);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Sending hint to {} interrupted", receiver);
        }
      }
      // all nodes in remote group know the hint, the hint can be removed
      if (hint.receivers.isEmpty()) {
        iterator.remove();
      }
    }
  }

  private boolean sendHint(Node receiver, PullSnapshotHint hint)
      throws TException, InterruptedException {
    boolean result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      result = sendHintsAsync(receiver, hint);
    } else {
      result = sendHintSync(receiver, hint);
    }
    return result;
  }

  private boolean sendHintsAsync(Node receiver, PullSnapshotHint hint)
      throws TException, InterruptedException {
    AsyncDataClient asyncDataClient = (AsyncDataClient) member.getAsyncClient(receiver);
    return SyncClientAdaptor.onSnapshotApplied(asyncDataClient, hint.header, hint.slots);
  }

  private boolean sendHintSync(Node receiver, PullSnapshotHint hint) throws TException {
    SyncDataClient syncDataClient = (SyncDataClient) member.getSyncClient(receiver);
    if (syncDataClient == null) {
      return false;
    }
    return syncDataClient.onSnapshotApplied(hint.header, hint.slots);
  }

  private static class PullSnapshotHint {

    /**
     * Nodes to send this hint;
     */
    private List<Node> receivers;

    private Node header;

    private List<Integer> slots;
  }
}
