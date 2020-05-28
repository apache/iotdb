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
import java.nio.file.Files;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import org.apache.iotdb.cluster.client.async.DataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.SnapshotApplicationException;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When a new node joins the cluster, a new data group is formed and some partitions are assigned
 * to the group. All members of the group should pull snapshots from the previous holders to
 * proceed the data transition.
 */
public class PullSnapshotTask implements Callable<Void> {

  public static final String TASK_SUFFIX = ".task";
  private static final Logger logger = LoggerFactory.getLogger(PullSnapshotTask.class);

  private PullSnapshotTaskDescriptor descriptor;
  private DataGroupMember newMember;

  private PullSnapshotRequest request;
  private SnapshotFactory snapshotFactory;

  private File snapshotSave;
  private Random random = new Random();

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

  private boolean pullSnapshot(int nodeIndex)
      throws InterruptedException, TException {
    Node node = descriptor.getPreviousHolders().get(nodeIndex);
    if (logger.isDebugEnabled()) {
      logger.debug("Pulling {} snapshots from {} of {}", descriptor.getSlots().size(), node,
          descriptor.getPreviousHolders().getHeader());
    }

    DataClient client =
        (DataClient) newMember.connectNode(node);
    if (client == null) {
      return false;
    }

    Map<Integer, Snapshot> result = SyncClientAdaptor.pullSnapshot(client, request,
        descriptor.getSlots(), snapshotFactory);
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
        newMember.applySnapshot(result);
        return true;
      } catch (SnapshotApplicationException e) {
        logger.error("Apply snapshot failed, retry...", e);
      }
    }
    return false;
  }

  @Override
  public Void call() {
    persistTask();
    request = new PullSnapshotRequest();
    request.setHeader(descriptor.getPreviousHolders().getHeader());
    request.setRequiredSlots(descriptor.getSlots());
    request.setRequireReadOnly(descriptor.isRequireReadOnly());


    boolean finished = false;
    int nodeIndex = -1;
    while (!finished) {
      try {
        // sequentially pick up a node that may have this slot
        nodeIndex = (nodeIndex + 1) % descriptor.getPreviousHolders().size();
        finished = pullSnapshot(nodeIndex);
        if (!finished) {
          Thread.sleep(ClusterConstant.PULL_SNAPSHOT_RETRY_INTERVAL);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when pulling slot {}", descriptor.getSlots(), e);
        finished = true;
      } catch (TException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Cannot pull slot {} from {}, retry", descriptor.getSlots(),
              descriptor.getPreviousHolders().get(nodeIndex), e);
        }
      }
    }
    removeTask();
    return null;
  }

  private void persistTask() {
    if (snapshotSave != null) {
      // the task is resumed from disk, do not persist it again
      return;
    }


    while (true) {
      String saveName = System.currentTimeMillis() + "_" + random.nextLong() + TASK_SUFFIX;
      snapshotSave = new File(newMember.getPullSnapshotTaskDir(), saveName);
      if (!snapshotSave.exists()) {
        snapshotSave.getParentFile().mkdirs();
        break;
      }
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
    try {
      Files.delete(snapshotSave.toPath());
    } catch (IOException e) {
      logger.warn("Cannot remove pull snapshot task file {}", snapshotSave, e);
    }
  }
}
