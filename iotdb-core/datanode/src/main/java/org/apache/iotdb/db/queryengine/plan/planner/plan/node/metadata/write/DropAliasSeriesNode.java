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

import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.mpp.rpc.thrift.TTimeSeriesInfo;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DropAliasSeriesNode extends PlanNode implements ISchemaRegionPlan {

  private final PartialPath aliasPath; // Alias path to drop
  private final PartialPath physicalPath; // Physical path (for rollback)
  private final TTimeSeriesInfo timeSeriesInfo; // Schema info for recreating alias series
  private final boolean isRollback; // Whether this is a rollback operation

  public DropAliasSeriesNode(PlanNodeId id, PartialPath aliasPath) {
    super(id);
    this.aliasPath = aliasPath;
    this.physicalPath = null;
    this.timeSeriesInfo = null;
    this.isRollback = false;
  }

  public DropAliasSeriesNode(
      PlanNodeId id,
      PartialPath aliasPath,
      PartialPath physicalPath,
      TTimeSeriesInfo timeSeriesInfo,
      boolean isRollback) {
    super(id);
    this.aliasPath = aliasPath;
    this.physicalPath = physicalPath;
    this.timeSeriesInfo = timeSeriesInfo;
    this.isRollback = isRollback;
  }

  public PartialPath getAliasPath() {
    return aliasPath;
  }

  public PartialPath getPhysicalPath() {
    return physicalPath;
  }

  public TTimeSeriesInfo getTimeSeriesInfo() {
    return timeSeriesInfo;
  }

  public boolean isRollback() {
    return isRollback;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("DropAliasSeriesNode does not support children");
  }

  @Override
  public PlanNode clone() {
    return new DropAliasSeriesNode(
        getPlanNodeId(), aliasPath, physicalPath, timeSeriesInfo, isRollback);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.DROP_ALIAS_SERIES;
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitDropAliasSeries(this, context);
  }

  @Override
  public <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitDropAliasSeries(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DROP_ALIAS_SERIES.serialize(byteBuffer);
    aliasPath.serialize(byteBuffer);
    // Serialize physicalPath
    if (physicalPath != null) {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      physicalPath.serialize(byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }
    // Serialize timeSeriesInfo
    if (timeSeriesInfo != null) {
      try {
        ReadWriteIOUtils.write((byte) 1, byteBuffer);
        ByteArrayOutputStream baos = new PublicBAOS();
        timeSeriesInfo.write(new TBinaryProtocol(new TIOStreamTransport(baos)));
        byte[] bytes = baos.toByteArray();
        ReadWriteIOUtils.write(bytes.length, byteBuffer);
        byteBuffer.put(bytes);
      } catch (TException e) {
        throw new ThriftSerDeException("Failed to serialize TTimeSeriesInfo: ", e);
      }
    } else {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }
    // Serialize isRollback
    ReadWriteIOUtils.write(isRollback, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DROP_ALIAS_SERIES.serialize(stream);
    aliasPath.serialize(stream);
    // Serialize physicalPath
    if (physicalPath != null) {
      ReadWriteIOUtils.write((byte) 1, stream);
      physicalPath.serialize(stream);
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }
    // Serialize timeSeriesInfo
    if (timeSeriesInfo != null) {
      ReadWriteIOUtils.write((byte) 1, stream);
      ByteArrayOutputStream baos = new PublicBAOS();
      try {
        timeSeriesInfo.write(new TBinaryProtocol(new TIOStreamTransport(baos)));
        byte[] bytes = baos.toByteArray();
        ReadWriteIOUtils.write(bytes.length, stream);
        stream.write(bytes);
      } catch (TException e) {
        throw new IOException("Failed to serialize TTimeSeriesInfo", e);
      }
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }
    // Serialize isRollback
    ReadWriteIOUtils.write(isRollback, stream);
  }

  public static DropAliasSeriesNode deserialize(ByteBuffer byteBuffer) {
    PartialPath aliasPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    // Deserialize physicalPath
    PartialPath physicalPath = null;
    byte hasPhysicalPath = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasPhysicalPath == 1) {
      physicalPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    }
    // Deserialize timeSeriesInfo
    TTimeSeriesInfo timeSeriesInfo = null;
    byte hasTimeSeriesInfo = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasTimeSeriesInfo == 1) {
      int length = ReadWriteIOUtils.readInt(byteBuffer);
      byte[] bytes = new byte[length];
      byteBuffer.get(bytes);
      try {
        timeSeriesInfo = new TTimeSeriesInfo();
        timeSeriesInfo.read(
            new TBinaryProtocol(new TIOStreamTransport(new ByteArrayInputStream(bytes))));
      } catch (TException e) {
        throw new ThriftSerDeException("Failed to deserialize TTimeSeriesInfo: ", e);
      }
    }
    // Deserialize isRollback
    boolean isRollback = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DropAliasSeriesNode(planNodeId, aliasPath, physicalPath, timeSeriesInfo, isRollback);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.DROP_ALIAS_SERIES;
  }

  @Override
  public String toString() {
    return String.format(
        "DropAliasSeriesNode-%s: drop alias series %s", getPlanNodeId(), aliasPath.getFullPath());
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
    DropAliasSeriesNode that = (DropAliasSeriesNode) o;
    return isRollback == that.isRollback
        && Objects.equals(aliasPath, that.aliasPath)
        && Objects.equals(physicalPath, that.physicalPath)
        && Objects.equals(timeSeriesInfo, that.timeSeriesInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), aliasPath, physicalPath, timeSeriesInfo, isRollback);
  }
}
