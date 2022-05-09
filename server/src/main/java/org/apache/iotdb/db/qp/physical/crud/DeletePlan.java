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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.DataRegion.TimePartitionFilter;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DeletePlan extends PhysicalPlan implements WALEntryValue {
  /** byte: type, integer: paths.size(), long: deleteStartTime, deleteEndTime, index */
  private static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + Integer.BYTES + Long.BYTES * 3;

  private long deleteStartTime;
  private long deleteEndTime;
  private List<PartialPath> paths = new ArrayList<>();
  /**
   * This deletion only affects those time partitions that evaluate true by the filter. If the
   * filter is null, all partitions are processed. This is to avoid redundant data deletions when
   * one timeseries deletion is split and executed into different replication groups.
   */
  private TimePartitionFilter partitionFilter;

  public DeletePlan() {
    super(Operator.OperatorType.DELETE);
  }

  /**
   * constructor of DeletePlan with single path.
   *
   * @param startTime delete time range start
   * @param endTime delete time range end
   * @param path time series path
   */
  public DeletePlan(long startTime, long endTime, PartialPath path) {
    super(Operator.OperatorType.DELETE);
    this.deleteStartTime = startTime;
    this.deleteEndTime = endTime;
    this.paths.add(path);
  }

  /**
   * constructor of DeletePlan with multiple paths.
   *
   * @param startTime delete time range start
   * @param endTime delete time range end
   * @param paths time series paths in List structure
   */
  public DeletePlan(long startTime, long endTime, List<PartialPath> paths) {
    super(Operator.OperatorType.DELETE);
    this.deleteStartTime = startTime;
    this.deleteEndTime = endTime;
    this.paths = paths;
  }

  public long getDeleteStartTime() {
    return deleteStartTime;
  }

  public void setDeleteStartTime(long delTime) {
    this.deleteStartTime = delTime;
  }

  public long getDeleteEndTime() {
    return deleteEndTime;
  }

  public void setDeleteEndTime(long delTime) {
    this.deleteEndTime = delTime;
  }

  public void addPath(PartialPath path) {
    this.paths.add(path);
  }

  public void addPaths(List<PartialPath> paths) {
    this.paths.addAll(paths);
  }

  @Override
  public List<PartialPath> getPaths() {
    return paths;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    this.paths = paths;
  }

  public TimePartitionFilter getPartitionFilter() {
    return partitionFilter;
  }

  public void setPartitionFilter(TimePartitionFilter partitionFilter) {
    this.partitionFilter = partitionFilter;
  }

  @Override
  public int hashCode() {
    return Objects.hash(deleteStartTime, deleteEndTime, paths);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeletePlan that = (DeletePlan) o;
    return deleteStartTime == that.deleteStartTime
        && deleteEndTime == that.deleteEndTime
        && Objects.equals(paths, that.paths);
  }

  @Override
  public int serializedSize() {
    int size = FIXED_SERIALIZED_SIZE;
    for (PartialPath path : paths) {
      size += ReadWriteIOUtils.sizeToWrite(path.getFullPath());
    }
    return size;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.DELETE.ordinal();
    stream.writeByte((byte) type);
    stream.writeLong(deleteStartTime);
    stream.writeLong(deleteEndTime);
    stream.writeInt(paths.size());
    for (PartialPath path : paths) {
      putString(stream, path.getFullPath());
    }

    stream.writeLong(index);
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    int type = PhysicalPlanType.DELETE.ordinal();
    buffer.put((byte) type);
    buffer.putLong(deleteStartTime);
    buffer.putLong(deleteEndTime);
    buffer.putInt(paths.size());
    for (PartialPath path : paths) {
      putString(buffer, path.getFullPath());
    }

    buffer.putLong(index);
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    int type = PhysicalPlanType.DELETE.ordinal();
    buffer.put((byte) type);
    buffer.putLong(deleteStartTime);
    buffer.putLong(deleteEndTime);
    buffer.putInt(paths.size());
    for (PartialPath path : paths) {
      WALWriteUtils.write(path.getFullPath(), buffer);
    }
    buffer.putLong(index);
  }

  @Override
  public void deserialize(DataInputStream stream) throws IOException, IllegalPathException {
    this.deleteStartTime = stream.readLong();
    this.deleteEndTime = stream.readLong();
    int pathSize = stream.readInt();
    this.paths = new ArrayList<>();
    for (int i = 0; i < pathSize; i++) {
      paths.add(new PartialPath(ReadWriteIOUtils.readString(stream)));
    }

    this.index = stream.readLong();
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.deleteStartTime = buffer.getLong();
    this.deleteEndTime = buffer.getLong();
    int pathSize = buffer.getInt();
    this.paths = new ArrayList<>();
    for (int i = 0; i < pathSize; i++) {
      paths.add(new PartialPath(readString(buffer)));
    }

    this.index = buffer.getLong();
  }
}
