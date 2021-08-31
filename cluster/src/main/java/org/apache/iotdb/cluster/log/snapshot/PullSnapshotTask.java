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

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.utils.ClientUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * When a new node joins the cluster, a new data group is formed and some partitions are assigned to
 * the group. All members of the group should pull snapshots from the previous holders to proceed
 * the data transition.
 */
public class PullSnapshotTask<T extends Snapshot> implements Callable<Void> {

  public static final String TASK_SUFFIX = ".task";
  private static final Logger logger = LoggerFactory.getLogger(PullSnapshotTask.class);

  private PullSnapshotTaskDescriptor descriptor;
  private DataGroupMember newMember;

  private PullSnapshotRequest request;
  private SnapshotFactory<T> snapshotFactory;

  private File snapshotSave;
  private Random random = new Random();

  /**
   * @param descriptor
   * @param newMember
   * @param snapshotFactory
   * @param snapshotSave if the task is resumed from a disk file, this should that file, otherwise
   *     it should bu null
   */
  public PullSnapshotTask(
      PullSnapshotTaskDescriptor descriptor,
      DataGroupMember newMember,
      SnapshotFactory<T> snapshotFactory,
      File snapshotSave) {
    this.descriptor = descriptor;
    this.newMember = newMember;
    this.snapshotFactory = snapshotFactory;
    this.snapshotSave = snapshotSave;
    persistTask();
  }

  @SuppressWarnings("java:S3740") // type cannot be known ahead
  private boolean pullSnapshot(int nodeIndex) throws InterruptedException, TException {
    Node node = descriptor.getPreviousHolders().get(nodeIndex);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Pulling slot {} and other {} snapshots from {} of {} for {}",
          descriptor.getSlots().get(0),
          descriptor.getSlots().size() - 1,
          node,
          descriptor.getPreviousHolders().getHeader(),
          newMember.getName());
    }

    Map<Integer, T> result = pullSnapshot(node);

    if (result != null) {
      // unlock slots that have no snapshots
      List<Integer> noSnapshotSlots = new ArrayList<>();
      for (Integer slot : descriptor.getSlots()) {
        if (!result.containsKey(slot)) {
          newMember.getSlotManager().setToNull(slot, false);
          noSnapshotSlots.add(slot);
        }
      }
      newMember.getSlotManager().save();
      if (!noSnapshotSlots.isEmpty() && logger.isInfoEnabled()) {
        logger.info(
            "{}: {} and other {} slots do not have snapshot",
            newMember.getName(),
            noSnapshotSlots.get(0),
            noSnapshotSlots.size() - 1);
      }

      if (logger.isInfoEnabled()) {
        logger.info(
            "{}: Received a snapshot {} from {}",
            newMember.getName(),
            result,
            descriptor.getPreviousHolders().get(nodeIndex));
      }
      try {
        if (result.size() > 0) {
          Snapshot snapshot = result.values().iterator().next();
          SnapshotInstaller installer = snapshot.getDefaultInstaller(newMember);
          installer.install(result, true);
        }
        // inform the previous holders that one member has successfully pulled snapshot
        newMember.registerPullSnapshotHint(descriptor);
        return true;
      } catch (SnapshotInstallationException e) {
        logger.error("Apply snapshot failed, retry...", e);
      }
    }
    return false;
  }

  private Map<Integer, T> pullSnapshot(Node node) throws TException, InterruptedException {
    Map<Integer, T> result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = (AsyncDataClient) newMember.getAsyncClient(node);
      if (client == null) {
        return null;
      }
      result =
          SyncClientAdaptor.pullSnapshot(client, request, descriptor.getSlots(), snapshotFactory);
    } else {
      SyncDataClient client = (SyncDataClient) newMember.getSyncClient(node);
      if (client == null) {
        return null;
      }
      PullSnapshotResp pullSnapshotResp;
      try {
        pullSnapshotResp = client.pullSnapshot(request);
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        throw e;
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
      result = new HashMap<>();
      for (Entry<Integer, ByteBuffer> integerByteBufferEntry :
          pullSnapshotResp.snapshotBytes.entrySet()) {
        T snapshot = snapshotFactory.create();
        snapshot.deserialize(integerByteBufferEntry.getValue());
        result.put(integerByteBufferEntry.getKey(), snapshot);
      }
    }
    return result;
  }

  @Override
  public Void call() {
    request = new PullSnapshotRequest();
    request.setHeader(descriptor.getPreviousHolders().getHeader());
    request.setRequiredSlots(descriptor.getSlots());
    request.setRequireReadOnly(descriptor.isRequireReadOnly());

    logger.info("{}: data migration starts.", newMember.getName());
    boolean finished = false;
    int nodeIndex = ((PartitionGroup) newMember.getAllNodes()).indexOf(newMember.getThisNode()) - 1;
    while (!finished) {
      try {
        // sequentially pick up a node that may have this slot
        nodeIndex = (nodeIndex + 1) % descriptor.getPreviousHolders().size();
        long startTime = System.currentTimeMillis();
        finished = pullSnapshot(nodeIndex);
        if (!finished) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "Cannot pull slot {} from {}, retry",
                descriptor.getSlots(),
                descriptor.getPreviousHolders().get(nodeIndex));
          }
          Thread.sleep(
              ClusterDescriptor.getInstance().getConfig().getPullSnapshotRetryIntervalMs());
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "{}: Data migration ends, cost {}ms",
                newMember,
                (System.currentTimeMillis() - startTime));
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        finished = true;
      } catch (TException e) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Cannot pull slot {} from {}, retry",
              descriptor.getSlots(),
              descriptor.getPreviousHolders().get(nodeIndex),
              e);
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
      logger.error(
          "Cannot save the pulling task: pull {} from {}",
          descriptor.getSlots(),
          descriptor.getPreviousHolders(),
          e);
    }
  }

  private void removeTask() {
    try {
      Files.delete(snapshotSave.toPath());
    } catch (IOException e) {
      logger.warn("Cannot remove pull snapshot task file {}", snapshotSave, e);
    }
  }

  public File getSnapshotSave() {
    return snapshotSave;
  }
}
