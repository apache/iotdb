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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.mpp.rpc.thrift.TTimeSeriesInfo;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class CreateAliasSeriesNode extends PlanNode implements ISchemaRegionPlan {

  private final PartialPath oldPath; // Physical path
  private final PartialPath newPath; // Alias path
  private final TTimeSeriesInfo timeSeriesInfo; // Schema info for creating alias series

  public CreateAliasSeriesNode(PlanNodeId id, PartialPath oldPath, PartialPath newPath) {
    super(id);
    this.oldPath = oldPath;
    this.newPath = newPath;
    this.timeSeriesInfo = null;
  }

  public CreateAliasSeriesNode(
      PlanNodeId id, PartialPath oldPath, PartialPath newPath, TTimeSeriesInfo timeSeriesInfo) {
    super(id);
    this.oldPath = oldPath;
    this.newPath = newPath;
    this.timeSeriesInfo = timeSeriesInfo;
  }

  public PartialPath getOldPath() {
    return oldPath;
  }

  public PartialPath getNewPath() {
    return newPath;
  }

  public TTimeSeriesInfo getTimeSeriesInfo() {
    return timeSeriesInfo;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new CreateAliasSeriesNode(getPlanNodeId(), oldPath, newPath, timeSeriesInfo);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.CREATE_ALIAS_SERIES;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitCreateAliasSeries(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.CREATE_ALIAS_SERIES.serialize(byteBuffer);
    oldPath.serialize(byteBuffer);
    newPath.serialize(byteBuffer);
    // Serialize timeSeriesInfo
    if (timeSeriesInfo != null) {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        timeSeriesInfo.write(new TBinaryProtocol(new TIOStreamTransport(baos)));
        byte[] bytes = baos.toByteArray();
        org.apache.tsfile.utils.ReadWriteIOUtils.write(bytes.length, byteBuffer);
        byteBuffer.put(bytes);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize TTimeSeriesInfo", e);
      }
    } else {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.CREATE_ALIAS_SERIES.serialize(stream);
    oldPath.serialize(stream);
    newPath.serialize(stream);
    // Serialize timeSeriesInfo
    if (timeSeriesInfo != null) {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 1, stream);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        timeSeriesInfo.write(new TBinaryProtocol(new TIOStreamTransport(baos)));
        byte[] bytes = baos.toByteArray();
        org.apache.tsfile.utils.ReadWriteIOUtils.write(bytes.length, stream);
        stream.write(bytes);
      } catch (Exception e) {
        throw new IOException("Failed to serialize TTimeSeriesInfo", e);
      }
    } else {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 0, stream);
    }
  }

  public static CreateAliasSeriesNode deserialize(ByteBuffer byteBuffer) {
    PartialPath oldPath =
        (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    PartialPath newPath =
        (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    // Deserialize timeSeriesInfo
    TTimeSeriesInfo timeSeriesInfo = null;
    byte hasTimeSeriesInfo = org.apache.tsfile.utils.ReadWriteIOUtils.readByte(byteBuffer);
    if (hasTimeSeriesInfo == 1) {
      int length = org.apache.tsfile.utils.ReadWriteIOUtils.readInt(byteBuffer);
      byte[] bytes = new byte[length];
      byteBuffer.get(bytes);
      try {
        timeSeriesInfo = new TTimeSeriesInfo();
        timeSeriesInfo.read(
            new TBinaryProtocol(new TIOStreamTransport(new ByteArrayInputStream(bytes))));
      } catch (Exception e) {
        throw new RuntimeException("Failed to deserialize TTimeSeriesInfo", e);
      }
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new CreateAliasSeriesNode(planNodeId, oldPath, newPath, timeSeriesInfo);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.CREATE_ALIAS_SERIES;
  }

  @Override
  public <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitCreateAliasSeries(this, context);
  }

  @Override
  public String toString() {
    return String.format(
        "CreateAliasSeriesNode-%s: %s -> %s",
        getPlanNodeId(), oldPath.getFullPath(), newPath.getFullPath());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    CreateAliasSeriesNode that = (CreateAliasSeriesNode) o;
    return Objects.equals(oldPath, that.oldPath)
        && Objects.equals(newPath, that.newPath)
        && Objects.equals(timeSeriesInfo, that.timeSeriesInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), oldPath, newPath, timeSeriesInfo);
  }
}
