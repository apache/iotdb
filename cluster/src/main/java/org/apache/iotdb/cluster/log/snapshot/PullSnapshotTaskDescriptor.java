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

import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.NodeSerializeUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * PullSnapshotTaskDescriptor describes a pull-snapshot-task with the slots to pull, the previous
 * owners and does this pulling require the provider to become read-only. So the task can be resumed
 * when system crashes.
 */
public class PullSnapshotTaskDescriptor {
  private PartitionGroup previousHolders;
  private List<Integer> slots;

  // set to true if the previous holder has been removed from the cluster.
  // This will make the previous holder read-only so that different new
  // replicas can pull the same snapshot.
  private boolean requireReadOnly;

  public PullSnapshotTaskDescriptor() {}

  public PullSnapshotTaskDescriptor(
      PartitionGroup previousOwners, List<Integer> slots, boolean requireReadOnly) {
    this.previousHolders = previousOwners;
    this.slots = slots;
    this.requireReadOnly = requireReadOnly;
  }

  public PartitionGroup getPreviousHolders() {
    return previousHolders;
  }

  public List<Integer> getSlots() {
    return slots;
  }

  public void setSlots(List<Integer> slots) {
    this.slots = slots;
  }

  boolean isRequireReadOnly() {
    return requireReadOnly;
  }

  public void serialize(DataOutputStream dataOutputStream) throws IOException {
    dataOutputStream.writeInt(slots.size());
    for (Integer slot : slots) {
      dataOutputStream.writeInt(slot);
    }

    dataOutputStream.writeInt(previousHolders.getId());
    dataOutputStream.writeInt(previousHolders.size());
    for (Node previousHolder : previousHolders) {
      NodeSerializeUtils.serialize(previousHolder, dataOutputStream);
    }

    dataOutputStream.writeBoolean(requireReadOnly);
  }

  public void deserialize(DataInputStream dataInputStream) throws IOException {
    int slotSize = dataInputStream.readInt();
    slots = new ArrayList<>(slotSize);
    for (int i = 0; i < slotSize; i++) {
      slots.add(dataInputStream.readInt());
    }

    previousHolders = new PartitionGroup(dataInputStream.readInt());
    int holderSize = dataInputStream.readInt();
    for (int i = 0; i < holderSize; i++) {
      Node node = new Node();
      NodeSerializeUtils.deserialize(node, dataInputStream);
      previousHolders.add(node);
    }

    requireReadOnly = dataInputStream.readBoolean();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PullSnapshotTaskDescriptor that = (PullSnapshotTaskDescriptor) o;
    return requireReadOnly == that.requireReadOnly
        && Objects.equals(previousHolders, that.previousHolders)
        && Objects.equals(slots, that.slots);
  }

  @Override
  public String toString() {
    return "PullSnapshotTaskDescriptor{"
        + " previousHolders="
        + previousHolders
        + ", slots="
        + slots
        + ", requireReadOnly="
        + requireReadOnly
        + "}";
  }

  @Override
  public int hashCode() {
    return Objects.hash(previousHolders, slots, requireReadOnly);
  }
}
