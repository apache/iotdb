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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FlushPlan extends PhysicalPlan {

  private static final Logger logger = LoggerFactory.getLogger(FlushPlan.class);
  /**
   * key-> storage group, value->list of pair, Pair<PartitionId, isSequence>,
   *
   * <p>Notice, the value maybe null, when it is null, all partitions under the storage groups are
   * flushed, so do not use {@link java.util.concurrent.ConcurrentHashMap} when initializing as
   * ConcurrentMap dose not support null key and value
   */
  private Map<PartialPath, List<Pair<Long, Boolean>>> storageGroupPartitionIds;

  // being null indicates flushing both seq and unseq data
  private Boolean isSeq;

  private boolean isSync;

  /** only for deserialize */
  public FlushPlan() {
    super(false, OperatorType.FLUSH);
  }

  public FlushPlan(Boolean isSeq, List<PartialPath> storageGroups) {
    super(false, OperatorType.FLUSH);
    if (storageGroups == null) {
      this.storageGroupPartitionIds = null;
    } else {
      this.storageGroupPartitionIds = new HashMap<>();
      for (PartialPath path : storageGroups) {
        this.storageGroupPartitionIds.put(path, null);
      }
    }
    this.isSeq = isSeq;
    this.isSync = false;
  }

  public FlushPlan(
      Boolean isSeq,
      boolean isSync,
      Map<PartialPath, List<Pair<Long, Boolean>>> storageGroupPartitionIds) {
    super(false, OperatorType.FLUSH);
    this.storageGroupPartitionIds = storageGroupPartitionIds;
    this.isSeq = isSeq;
    this.isSync = isSync;
  }

  public Boolean isSeq() {
    return isSeq;
  }

  public boolean isSync() {
    return isSync;
  }

  @Override
  public List<PartialPath> getPaths() {
    if (storageGroupPartitionIds == null) {
      return Collections.emptyList();
    }
    List<PartialPath> ret = new ArrayList<>();
    for (Entry<PartialPath, List<Pair<Long, Boolean>>> entry :
        storageGroupPartitionIds.entrySet()) {
      ret.add(entry.getKey());
    }
    return ret;
  }

  public Map<PartialPath, List<Pair<Long, Boolean>>> getStorageGroupPartitionIds() {
    return storageGroupPartitionIds;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.FLUSH.ordinal());
    if (isSeq == null) {
      stream.writeByte(2);
    } else {
      stream.writeByte(Boolean.TRUE.equals(isSeq) ? 1 : 0);
    }

    stream.writeByte(isSync ? 1 : 0);
    writeStorageGroupPartitionIds(stream);
  }

  public void writeStorageGroupPartitionIds(DataOutputStream stream) throws IOException {
    if (storageGroupPartitionIds == null) {
      // null value
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      stream.writeInt(storageGroupPartitionIds.size());
      for (Entry<PartialPath, List<Pair<Long, Boolean>>> entry :
          storageGroupPartitionIds.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey().getFullPath(), stream);
        if (entry.getValue() == null) {
          // null value
          stream.write((byte) 0);
        } else {
          stream.write((byte) 1);
          ReadWriteIOUtils.write(entry.getValue().size(), stream);
          for (Pair<Long, Boolean> pair : entry.getValue()) {
            ReadWriteIOUtils.write(pair.left, stream);
            ReadWriteIOUtils.write(pair.right, stream);
          }
        }
      }
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.FLUSH.ordinal();
    buffer.put((byte) type);
    if (isSeq == null) {
      buffer.put((byte) 2);
    } else {
      buffer.put((byte) (Boolean.TRUE.equals(isSeq) ? 1 : 0));
    }
    buffer.put((byte) (isSync ? 1 : 0));
    writeStorageGroupPartitionIds(buffer);
  }

  public void writeStorageGroupPartitionIds(ByteBuffer buffer) {
    if (storageGroupPartitionIds == null) {
      // null value
      buffer.put((byte) 0);
    } else {
      // null value
      buffer.put((byte) 1);
      buffer.putInt(storageGroupPartitionIds.size());
      for (Entry<PartialPath, List<Pair<Long, Boolean>>> entry :
          storageGroupPartitionIds.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey().getFullPath(), buffer);
        if (entry.getValue() == null) {
          // null value
          buffer.put((byte) 0);
        } else {
          buffer.put((byte) 1);
          ReadWriteIOUtils.write(entry.getValue().size(), buffer);
          for (Pair<Long, Boolean> pair : entry.getValue()) {
            ReadWriteIOUtils.write(pair.left, buffer);
            ReadWriteIOUtils.write(pair.right, buffer);
          }
        }
      }
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    byte isSeqFlag = buffer.get();
    if (isSeqFlag == 2) {
      this.isSeq = null;
    } else {
      this.isSeq = isSeqFlag == 1;
    }
    this.isSync = buffer.get() == 1;
    readStorageGroupPartitionIds(buffer);
  }

  private void readStorageGroupPartitionIds(ByteBuffer buffer) {
    byte flag = buffer.get();
    if (flag == 0) {
      this.storageGroupPartitionIds = null;
    } else {
      int storageGroupsMapSize = buffer.getInt();
      this.storageGroupPartitionIds = new HashMap<>(storageGroupsMapSize);
      for (int i = 0; i < storageGroupsMapSize; i++) {
        PartialPath tmpPath = null;
        try {
          tmpPath = new PartialPath(ReadWriteIOUtils.readString(buffer));
        } catch (IllegalPathException e) {
          logger.error("Illegal path found during FlushPlan serialization:", e);
        }
        flag = buffer.get();
        if (flag == 0) {
          storageGroupPartitionIds.put(tmpPath, null);
        } else {
          int partitionIdSize = ReadWriteIOUtils.readInt(buffer);
          List<Pair<Long, Boolean>> partitionIdList = new ArrayList<>(partitionIdSize);
          for (int j = 0; j < partitionIdSize; j++) {
            long partitionId = ReadWriteIOUtils.readLong(buffer);
            Boolean partitionIsSeq = ReadWriteIOUtils.readBool(buffer);
            Pair<Long, Boolean> tmpPair = new Pair<>(partitionId, partitionIsSeq);
            partitionIdList.add(tmpPair);
          }
          storageGroupPartitionIds.put(tmpPath, partitionIdList);
        }
      }
    }
  }

  @Override
  public String toString() {
    return "FlushPlan{"
        + " storageGroupPartitionIds="
        + storageGroupPartitionIds
        + ", isSeq="
        + isSeq
        + ", isSync="
        + isSync
        + "}";
  }
}
