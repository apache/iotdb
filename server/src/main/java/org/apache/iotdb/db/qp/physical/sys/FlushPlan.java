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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class FlushPlan extends PhysicalPlan {

  /**
   * key-> storage group, value->list of pair, Pair<PartitionId, isSequence>
   */
  private Map<Path, List<Pair<Long, Boolean>>> storageGroupPartitionIds;

  private Boolean isSeq;

  private boolean isSync;

  /**
   * only for deserialize
   */
  public FlushPlan() {
    super(false, OperatorType.FLUSH);
  }

  public FlushPlan(Boolean isSeq, List<Path> storageGroups) {
    super(false, OperatorType.FLUSH);
    if (storageGroups == null) {
      this.storageGroupPartitionIds = null;
    } else {
      this.storageGroupPartitionIds = new ConcurrentHashMap<>();
      for (Path path : storageGroups) {
        this.storageGroupPartitionIds.put(path, null);
      }
    }
    this.isSeq = isSeq;
    this.isSync = false;
  }

  public FlushPlan(Boolean isSeq, boolean isSync,
      Map<Path, List<Pair<Long, Boolean>>> storageGroupPartitionIds) {
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
  public List<Path> getPaths() {
    if (storageGroupPartitionIds == null) {
      return null;
    }
    List<Path> ret = new ArrayList<>();
    for (Entry<Path, List<Pair<Long, Boolean>>> entry : storageGroupPartitionIds.entrySet()) {
      ret.add(entry.getKey());
    }
    return ret;
  }

  @Override
  public List<String> getPathsStrings() {
    List<String> ret = new ArrayList<>();
    for (Entry<Path, List<Pair<Long, Boolean>>> entry : storageGroupPartitionIds.entrySet()) {
      ret.add(entry.getKey().getFullPath());
    }
    return ret;
  }

  public Map<Path, List<Pair<Long, Boolean>>> getStorageGroupPartitionIds() {
    return storageGroupPartitionIds;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.FLUSH.ordinal());
    stream.writeByte((isSeq == null || !isSeq) ? 0 : 1);
    stream.writeByte(isSync ? 1 : 0);
    if (storageGroupPartitionIds == null) {
      // null value
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      stream.writeInt(storageGroupPartitionIds.size());
      for (Entry<Path, List<Pair<Long, Boolean>>> entry : storageGroupPartitionIds.entrySet()) {
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
    buffer.put((byte) ((isSeq == null || !isSeq) ? 0 : 1));
    buffer.put((byte) (isSync ? 1 : 0));
    if (storageGroupPartitionIds == null) {
      // null value
      buffer.put((byte) 0);
    } else {
      // null value
      buffer.put((byte) 1);
      buffer.putInt(storageGroupPartitionIds.size());
      for (Entry<Path, List<Pair<Long, Boolean>>> entry : storageGroupPartitionIds.entrySet()) {
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
    this.isSeq = (buffer.get() == 1) ? true : null;
    this.isSync = buffer.get() == 1;
    byte flag = buffer.get();
    if (flag == 0) {
      this.storageGroupPartitionIds = null;
    } else {
      int storageGroupsMapSize = buffer.getInt();
      this.storageGroupPartitionIds = new HashMap<>(storageGroupsMapSize);
      for (int i = 0; i < storageGroupsMapSize; i++) {
        Path tmpPath = new Path(ReadWriteIOUtils.readString(buffer));
        flag = buffer.get();
        if (flag == 0) {
          storageGroupPartitionIds.put(tmpPath, null);
        } else {
          int partitionIdSize = ReadWriteIOUtils.readInt(buffer);
          List<Pair<Long, Boolean>> partitionIdList = new ArrayList<>(partitionIdSize);
          for (int j = 0; j < partitionIdSize; j++) {
            long partitionId = ReadWriteIOUtils.readLong(buffer);
            Boolean isSeq = ReadWriteIOUtils.readBool(buffer);
            Pair<Long, Boolean> tmpPair = new Pair<>(partitionId, isSeq);
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
        + " storageGroupPartitionIds=" + storageGroupPartitionIds
        + ", isSeq=" + isSeq
        + ", isSync=" + isSync
        + "}";
  }
}
