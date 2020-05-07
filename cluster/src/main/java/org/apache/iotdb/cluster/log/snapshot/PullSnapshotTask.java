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

package org.apache.iotdb.cluster.log.snapshot;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.SnapshotApplicationException;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.PullSnapshotHandler;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When a new node joins the cluster, a new data group is formed and some partitions are assigned
 * to the group. All members of the group should pull snapshots from the previous holders to
 * proceed the data transition.
 */
public class PullSnapshotTask<T extends Snapshot> implements Callable<Map<Integer,
    T>> {

  public static final String TASK_SUFFIX = ".task";
  private static final Logger logger = LoggerFactory.getLogger(PullSnapshotTask.class);

  private PullSnapshotTaskDescriptor descriptor;
  private DataGroupMember newMember;

  private PullSnapshotRequest request;
  private SnapshotFactory snapshotFactory;

  private File snapshotSave;

  /**
   *
   * @param descriptor
   * @param newMember
   * @param snapshotFactory
   * @param snapshotSave if the task is resumed from a disk file, this should that file,
   *                     otherwise it should bu null
   */
  public PullSnapshotTask(PullSnapshotTaskDescriptor descriptor,
      DataGroupMember newMember, SnapshotFactory snapshotFactory, File snapshotSave) {
    this.descriptor = descriptor;
    this.newMember = newMember;
    this.snapshotFactory = snapshotFactory;
    this.snapshotSave = snapshotSave;
  }

  private boolean pullSnapshot(AtomicReference<Map<Integer, T>> snapshotRef, int nodeIndex)
      throws InterruptedException, TException {
    Node node = descriptor.getPreviousHolders().get(nodeIndex);
    logger.debug("Pulling {} snapshots from {}", descriptor.getSlots().size(), node);

    DataClient client =
        (DataClient) newMember.connectNode(node);
    if (client == null) {
      // network is bad, wait and retry
      Thread.sleep(ClusterConstant.PULL_SNAPSHOT_RETRY_INTERVAL);
    } else {
      synchronized (snapshotRef) {
        client.pullSnapshot(request, new PullSnapshotHandler<>(snapshotRef,
            node, descriptor.getSlots(), snapshotFactory));
        snapshotRef.wait(RaftServer.connectionTimeoutInMS);
      }
      Map<Integer, T> result = snapshotRef.get();
      if (result != null) {
        // unlock slots that have no snapshots
        for (Integer slot : descriptor.getSlots()) {
          if (!result.containsKey(slot)) {
            newMember.getSlotManager().setToNull(slot);
          }
        }

        if (logger.isInfoEnabled()) {
          logger.info("Received a snapshot {} from {}", result, descriptor.getPreviousHolders().get(nodeIndex));
        }
        try {
          newMember.applySnapshot((Map<Integer, Snapshot>) result);
        } catch (SnapshotApplicationException e) {
          logger.error("Apply snapshot failed, retry...", e);
          Thread.sleep(ClusterConstant.PULL_SNAPSHOT_RETRY_INTERVAL);
          return false;
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
    persistTask();
    request = new PullSnapshotRequest();
    request.setHeader(descriptor.getPreviousHolders().getHeader());
    request.setRequiredSlots(descriptor.getSlots());
    request.setRequireReadOnly(descriptor.isRequireReadOnly());
    AtomicReference<Map<Integer, T>> snapshotRef = new AtomicReference<>();
    boolean finished = false;
    int nodeIndex = -1;
    while (!finished) {
      try {
        // sequentially pick up a node that may have this slot
        nodeIndex = (nodeIndex + 1) % descriptor.getPreviousHolders().size();
        finished = pullSnapshot(snapshotRef, nodeIndex);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when pulling slot {}", descriptor.getSlots(), e);
        finished = true;
      } catch (TException e) {
        logger.debug("Cannot pull slot {} from {}, retry", descriptor.getSlots(),
            descriptor.getPreviousHolders().get(nodeIndex), e);
      }
    }
    removeTask();
    return snapshotRef.get();
  }

  private void persistTask() {
    if (snapshotSave != null) {
      // the task is resumed from disk, do not persist it again
      return;
    }

    Random random = new Random();
    while (true) {
      String saveName = System.currentTimeMillis() + "_" + random.nextLong() + ".task";
      snapshotSave = new File(newMember.getPullSnapshotTaskDir(), saveName);
      if (snapshotSave.exists()) {
        continue;
      }
      snapshotSave.getParentFile().mkdirs();
      break;
    }

    try (DataOutputStream dataOutputStream =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(snapshotSave)))) {
      descriptor.serialize(dataOutputStream);
    } catch (IOException e) {
      logger.error("Cannot save the pulling task: pull {} from {}", descriptor.getSlots(),
          descriptor.getPreviousHolders(), e);
    }
  }

  private void removeTask() {
    snapshotSave.delete();
  }
}
