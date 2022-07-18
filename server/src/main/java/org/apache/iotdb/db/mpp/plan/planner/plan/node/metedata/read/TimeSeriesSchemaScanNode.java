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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class TimeSeriesSchemaScanNode extends SchemaQueryScanNode {

  private final String key;
  private final String value;
  private final boolean isContains;

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private final boolean orderByHeat;

  public TimeSeriesSchemaScanNode(
      PlanNodeId id,
      PartialPath partialPath,
      String key,
      String value,
      int limit,
      int offset,
      boolean orderByHeat,
      boolean isContains,
      boolean isPrefixPath) {
    super(id, partialPath, limit, offset, isPrefixPath);
    this.key = key;
    this.value = value;
    this.orderByHeat = orderByHeat;
    this.isContains = isContains;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TIME_SERIES_SCHEMA_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    ReadWriteIOUtils.write(key, byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
    ReadWriteIOUtils.write(limit, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
    ReadWriteIOUtils.write(orderByHeat, byteBuffer);
    ReadWriteIOUtils.write(isContains, byteBuffer);
    ReadWriteIOUtils.write(isPrefixPath, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TIME_SERIES_SCHEMA_SCAN.serialize(stream);
    ReadWriteIOUtils.write(path.getFullPath(), stream);
    ReadWriteIOUtils.write(key, stream);
    ReadWriteIOUtils.write(value, stream);
    ReadWriteIOUtils.write(limit, stream);
    ReadWriteIOUtils.write(offset, stream);
    ReadWriteIOUtils.write(orderByHeat, stream);
    ReadWriteIOUtils.write(isContains, stream);
    ReadWriteIOUtils.write(isPrefixPath, stream);
  }

  public static TimeSeriesSchemaScanNode deserialize(ByteBuffer byteBuffer) {
    String fullPath = ReadWriteIOUtils.readString(byteBuffer);
    PartialPath path;
    try {
      path = new PartialPath(fullPath);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize TimeSeriesSchemaScanNode", e);
    }
    String key = ReadWriteIOUtils.readString(byteBuffer);
    String value = ReadWriteIOUtils.readString(byteBuffer);
    int limit = ReadWriteIOUtils.readInt(byteBuffer);
    int offset = ReadWriteIOUtils.readInt(byteBuffer);
    boolean oderByHeat = ReadWriteIOUtils.readBool(byteBuffer);
    boolean isContains = ReadWriteIOUtils.readBool(byteBuffer);
    boolean isPrefixPath = ReadWriteIOUtils.readBool(byteBuffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    return new TimeSeriesSchemaScanNode(
        planNodeId, path, key, value, limit, offset, oderByHeat, isContains, isPrefixPath);
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public boolean isContains() {
    return isContains;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  @Override
  public PlanNode clone() {
    return new TimeSeriesSchemaScanNode(
        getPlanNodeId(), path, key, value, limit, offset, orderByHeat, isContains, isPrefixPath);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return HeaderConstant.showTimeSeriesHeader.getRespColumns();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TimeSeriesSchemaScanNode that = (TimeSeriesSchemaScanNode) o;
    return isContains == that.isContains
        && orderByHeat == that.orderByHeat
        && Objects.equals(key, that.key)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), key, value, isContains, orderByHeat);
  }

  @Override
  public String toString() {
    return String.format(
        "TimeSeriesSchemaScanNode-%s:[DataRegion: %s]",
        this.getPlanNodeId(), this.getRegionReplicaSet());
  }
}
