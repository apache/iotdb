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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ActiveRegionScanMergeNode extends MultiChildProcessNode {

  private final boolean outputCount;

  private final boolean needMerge;

  private final long estimatedSize;

  public ActiveRegionScanMergeNode(
      PlanNodeId id, boolean outputCount, boolean needMerge, long size) {
    super(id);
    this.estimatedSize = size;
    this.outputCount = outputCount;
    this.needMerge = needMerge;
  }

  public long getEstimatedSize() {
    return estimatedSize;
  }

  public boolean isOutputCount() {
    return outputCount;
  }

  public boolean isNeedMerge() {
    return needMerge;
  }

  @Override
  public PlanNode clone() {
    return new ActiveRegionScanMergeNode(this.id, outputCount, needMerge, estimatedSize);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputCount
        ? ColumnHeaderConstant.countDevicesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList())
        : ColumnHeaderConstant.showDevicesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList());
  }

  public static ActiveRegionScanMergeNode deserialize(ByteBuffer byteBuffer) {
    boolean outputCount = ReadWriteIOUtils.readBool(byteBuffer);
    boolean needMerge = ReadWriteIOUtils.readBool(byteBuffer);
    long estimatedSize = ReadWriteIOUtils.readLong(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new ActiveRegionScanMergeNode(planNodeId, outputCount, needMerge, estimatedSize);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.REGION_MERGE.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputCount, byteBuffer);
    ReadWriteIOUtils.write(needMerge, byteBuffer);
    ReadWriteIOUtils.write(estimatedSize, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.REGION_MERGE.serialize(stream);
    ReadWriteIOUtils.write(outputCount, stream);
    ReadWriteIOUtils.write(needMerge, stream);
    ReadWriteIOUtils.write(estimatedSize, stream);
  }

  @Override
  public String toString() {
    return "ActiveRegionScanMergeNode{"
        + "outputCount="
        + outputCount
        + ", needMerge="
        + needMerge
        + ", id="
        + id
        + '}';
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitRegionMerge(this, context);
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
    ActiveRegionScanMergeNode that = (ActiveRegionScanMergeNode) o;
    return outputCount == that.outputCount && needMerge == that.needMerge;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), outputCount, needMerge);
  }
}
