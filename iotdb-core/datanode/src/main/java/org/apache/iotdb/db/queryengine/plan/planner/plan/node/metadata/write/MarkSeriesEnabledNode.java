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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MarkSeriesEnabledNode extends PlanNode implements ISchemaRegionPlan {

  private final PartialPath physicalPath; // Physical path to mark as enabled
  private final PartialPath aliasPath; // Original alias path (for rollback)
  private final TTimeSeriesInfo timeSeriesInfo; // Schema info for updating physical series
  private final boolean isRollback; // True if this is a rollback operation

  public MarkSeriesEnabledNode(PlanNodeId id, PartialPath physicalPath) {
    super(id);
    this.physicalPath = physicalPath;
    this.aliasPath = null;
    this.timeSeriesInfo = null;
    this.isRollback = false;
  }

  public MarkSeriesEnabledNode(
      PlanNodeId id,
      PartialPath physicalPath,
      PartialPath aliasPath,
      TTimeSeriesInfo timeSeriesInfo,
      boolean isRollback) {
    super(id);
    this.physicalPath = physicalPath;
    this.aliasPath = aliasPath;
    this.timeSeriesInfo = timeSeriesInfo;
    this.isRollback = isRollback;
  }

  public PartialPath getPhysicalPath() {
    return physicalPath;
  }

  public PartialPath getAliasPath() {
    return aliasPath;
  }

  public TTimeSeriesInfo getTimeSeriesInfo() {
    return timeSeriesInfo;
  }

  public boolean isRollback() {
    return isRollback;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("MarkSeriesEnabledNode does not support children");
  }

  @Override
  public PlanNode clone() {
    return new MarkSeriesEnabledNode(
        getPlanNodeId(), physicalPath, aliasPath, timeSeriesInfo, isRollback);
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
    return PlanNodeType.MARK_SERIES_ENABLED;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.MARK_SERIES_ENABLED.serialize(byteBuffer);
    physicalPath.serialize(byteBuffer);
    // Serialize aliasPath
    if (aliasPath != null) {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 1, byteBuffer);
      aliasPath.serialize(byteBuffer);
    } else {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }
    // Serialize timeSeriesInfo
    if (timeSeriesInfo != null) {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        timeSeriesInfo.write(new TBinaryProtocol(new TIOStreamTransport(baos)));
        byte[] bytes = baos.toByteArray();
        org.apache.tsfile.utils.ReadWriteIOUtils.write(bytes.length, byteBuffer);
        byteBuffer.put(bytes);
      } catch (TException e) {
        throw new ThriftSerDeException("Failed to serialize TTimeSeriesInfo: ", e);
      }
    } else {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }
    // Serialize isRollback
    org.apache.tsfile.utils.ReadWriteIOUtils.write(isRollback, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.MARK_SERIES_ENABLED.serialize(stream);
    physicalPath.serialize(stream);
    // Serialize aliasPath
    if (aliasPath != null) {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 1, stream);
      aliasPath.serialize(stream);
    } else {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 0, stream);
    }
    // Serialize timeSeriesInfo
    if (timeSeriesInfo != null) {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 1, stream);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        timeSeriesInfo.write(new TBinaryProtocol(new TIOStreamTransport(baos)));
        byte[] bytes = baos.toByteArray();
        org.apache.tsfile.utils.ReadWriteIOUtils.write(bytes.length, stream);
        stream.write(bytes);
      } catch (TException e) {
        throw new IOException("Failed to serialize TTimeSeriesInfo", e);
      }
    } else {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 0, stream);
    }
    // Serialize isRollback
    org.apache.tsfile.utils.ReadWriteIOUtils.write(isRollback, stream);
  }

  public static MarkSeriesEnabledNode deserialize(ByteBuffer byteBuffer) {
    PartialPath physicalPath =
        (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    // Deserialize aliasPath
    PartialPath aliasPath = null;
    byte hasAliasPath = org.apache.tsfile.utils.ReadWriteIOUtils.readByte(byteBuffer);
    if (hasAliasPath == 1) {
      aliasPath =
          (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    }
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
      } catch (TException e) {
        throw new ThriftSerDeException("Failed to deserialize TTimeSeriesInfo: ", e);
      }
    }
    // Deserialize isRollback
    boolean isRollback = org.apache.tsfile.utils.ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new MarkSeriesEnabledNode(
        planNodeId, physicalPath, aliasPath, timeSeriesInfo, isRollback);
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitMarkSeriesEnabled(this, context);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.MARK_SERIES_ENABLED;
  }

  @Override
  public <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitMarkSeriesEnabled(this, context);
  }

  @Override
  public String toString() {
    return String.format(
        "MarkSeriesEnabledNode-%s: mark series enabled %s (remove DISABLED, clear ALIAS_PATH)",
        getPlanNodeId(), physicalPath.getFullPath());
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
    MarkSeriesEnabledNode that = (MarkSeriesEnabledNode) o;
    return isRollback == that.isRollback
        && Objects.equals(physicalPath, that.physicalPath)
        && Objects.equals(aliasPath, that.aliasPath)
        && Objects.equals(timeSeriesInfo, that.timeSeriesInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), physicalPath, aliasPath, timeSeriesInfo, isRollback);
  }
}
