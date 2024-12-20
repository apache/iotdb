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

package org.apache.iotdb.db.pipe.consensus.deletion;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.pipe.datastructure.PersistentResource;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteNodeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * DeletionResource is designed for IoTConsensusV2 to manage the lifecycle of all deletion
 * operations including realtime deletion and historical deletion. In order to be compatible with
 * user pipe framework, PipeConsensus will use {@link PipeDeleteDataNodeEvent}
 */
public class DeletionResource implements PersistentResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletionResource.class);
  private final Consumer<DeletionResource> removeHook;
  private final AtomicInteger pipeTaskReferenceCount;
  private final AbstractDeleteDataNode deleteDataNode;
  private final ConsensusGroupId consensusGroupId;
  private volatile Status currentStatus;

  // it's safe to use volatile here to make this reference thread-safe.
  @SuppressWarnings("squid:S3077")
  private volatile Exception cause;

  public DeletionResource(
      AbstractDeleteDataNode deleteDataNode,
      Consumer<DeletionResource> removeHook,
      String regionId) {
    this.deleteDataNode = deleteDataNode;
    this.removeHook = removeHook;
    this.currentStatus = Status.RUNNING;
    this.consensusGroupId =
        ConsensusGroupId.Factory.create(
            TConsensusGroupType.DataRegion.getValue(), Integer.parseInt(regionId));
    this.pipeTaskReferenceCount =
        new AtomicInteger(
            DataRegionConsensusImpl.getInstance().getReplicationNum(consensusGroupId) - 1);
  }

  public synchronized void decreaseReference() {
    if (pipeTaskReferenceCount.get() == 1) {
      removeSelf();
    }
    pipeTaskReferenceCount.decrementAndGet();
  }

  public void removeSelf() {
    LOGGER.info("DeletionResource {} has been released, trigger a remove of DAL...", this);
    removeHook.accept(this);
  }

  public long getReferenceCount() {
    return pipeTaskReferenceCount.get();
  }

  public synchronized void onPersistFailed(Exception e) {
    cause = e;
    currentStatus = Status.FAILURE;
    this.notifyAll();
  }

  public synchronized void onPersistSucceed() {
    currentStatus = Status.SUCCESS;
    this.notifyAll();
  }

  /**
   * @return true, if this object has been successfully persisted, false if persist failed.
   */
  public synchronized Status waitForResult() {
    while (currentStatus == Status.RUNNING) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted when waiting for result.", e);
        Thread.currentThread().interrupt();
        currentStatus = Status.FAILURE;
        break;
      }
    }
    return currentStatus;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return deleteDataNode.getProgressIndex();
  }

  /**
   * Only the default implementation is provided here, which will not be called in practice, but is
   * only used to implement the interface
   */
  @Override
  public long getFileStartTime() {
    return 0;
  }

  /**
   * Only the default implementation is provided here, which will not be called in practice, but is
   * only used to implement the interface
   */
  @Override
  public long getFileEndTime() {
    return 0;
  }

  public AbstractDeleteDataNode getDeleteDataNode() {
    return deleteDataNode;
  }

  public ByteBuffer serialize() {
    ByteBuffer deletion = deleteDataNode.serializeToDAL();
    final ByteBuffer result = ByteBuffer.allocate(deletion.limit());
    result.put(deletion);
    return result;
  }

  public static DeletionResource deserialize(
      final ByteBuffer buffer, final String regionId, final Consumer<DeletionResource> removeHook)
      throws IOException {
    AbstractDeleteDataNode node = DeleteNodeType.deserializeFromDAL(buffer);
    return new DeletionResource(node, removeHook, regionId);
  }

  public static boolean isDeleteNodeGeneratedInLocalByIoTV2(AbstractDeleteDataNode node) {
    if (node.getProgressIndex() instanceof RecoverProgressIndex) {
      RecoverProgressIndex recoverProgressIndex = (RecoverProgressIndex) node.getProgressIndex();
      return recoverProgressIndex
          .getDataNodeId2LocalIndex()
          .containsKey(IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format(
        "DeletionResource[%s]{referenceCount=%s}", deleteDataNode, getReferenceCount());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DeletionResource otherEvent = (DeletionResource) o;
    return Objects.equals(deleteDataNode, otherEvent.deleteDataNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deleteDataNode);
  }

  public Exception getCause() {
    return cause;
  }

  public enum Status {
    SUCCESS,
    FAILURE,
    RUNNING,
  }
}
