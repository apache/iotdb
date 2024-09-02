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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaSerializableEventType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * DeletionResource is designed for PipeConsensus to manage the lifecycle of all deletion operations
 * including realtime deletion and historical deletion. In order to be compatible with user pipe
 * framework, PipeConsensus will use {@link PipeSchemaRegionWritePlanEvent}
 */
public class DeletionResource {
  private final Consumer<DeletionResource> removeHook;
  private final AtomicLong latestUpdateTime;
  private PipeSchemaRegionWritePlanEvent deletionEvent;

  public DeletionResource(
      PipeSchemaRegionWritePlanEvent deletionEvent, Consumer<DeletionResource> removeHook) {
    this.deletionEvent = deletionEvent;
    this.removeHook = removeHook;
    latestUpdateTime = new AtomicLong(System.currentTimeMillis());
  }

  /**
   * This method is invoked when DeletionResource is deleted by DeleteResourceManager. In this
   * method, we release the reference of deletionEvent to resolve circular references between
   * deletionResource and deletionEvent so that GC can reclaim them.
   */
  public void releaseSelf() {
    deletionEvent = null;
  }

  public void removeSelf() {
    removeHook.accept(this);
  }

  public void increaseReferenceCount() {
    deletionEvent.increaseReferenceCount(DeletionResource.class.getSimpleName());
    updateLatestUpdateTime();
  }

  public void decreaseReferenceCount() {
    deletionEvent.decreaseReferenceCount(DeletionResource.class.getSimpleName(), false);
  }

  public long getReferenceCount() {
    return deletionEvent.getReferenceCount();
  }

  public long getLatestUpdateTime() {
    return latestUpdateTime.get();
  }

  public ProgressIndex getProgressIndex() {
    return ((DeleteDataNode) deletionEvent.getPlanNode()).getProgressIndex();
  }

  public PipeSchemaRegionWritePlanEvent getDeletionEvent() {
    return deletionEvent;
  }

  public ByteBuffer serialize() {
    return deletionEvent.serializeToByteBuffer();
  }

  public static DeletionResource deserialize(
      final ByteBuffer buffer, final Consumer<DeletionResource> removeHook) throws IOException {
    PipeSchemaRegionWritePlanEvent event =
        (PipeSchemaRegionWritePlanEvent) PipeSchemaSerializableEventType.deserialize(buffer);
    return new DeletionResource(event, removeHook);
  }

  private void updateLatestUpdateTime() {
    latestUpdateTime.set(System.currentTimeMillis());
  }

  @Override
  public String toString() {
    return String.format(
        "DeletionResource[%s]{referenceCount=%s, latestUpdateTime=%s}",
        deletionEvent, getReferenceCount(), getLatestUpdateTime());
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
    return Objects.equals(deletionEvent, otherEvent.deletionEvent)
        && latestUpdateTime.get() == otherEvent.latestUpdateTime.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(deletionEvent, latestUpdateTime);
  }
}
