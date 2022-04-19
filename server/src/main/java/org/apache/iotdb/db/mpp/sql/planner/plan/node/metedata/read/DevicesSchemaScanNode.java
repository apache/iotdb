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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class DevicesSchemaScanNode extends SchemaScanNode {

  private final boolean hasSgCol;

  public DevicesSchemaScanNode(
      PlanNodeId id,
      PartialPath path,
      int limit,
      int offset,
      boolean isPrefixPath,
      boolean hasSgCol) {
    super(id, path, limit, offset, isPrefixPath);
    this.hasSgCol = hasSgCol;
  }

  public boolean isHasSgCol() {
    return hasSgCol;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new DevicesSchemaScanNode(getPlanNodeId(), path, limit, offset, isPrefixPath, hasSgCol);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICES_SCHEMA_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    ReadWriteIOUtils.write(limit, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
    ReadWriteIOUtils.write(isPrefixPath, byteBuffer);
    ReadWriteIOUtils.write(hasSgCol, byteBuffer);
  }

  public static DevicesSchemaScanNode deserialize(ByteBuffer byteBuffer) {
    String fullPath = ReadWriteIOUtils.readString(byteBuffer);
    PartialPath path;
    try {
      path = new PartialPath(fullPath);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize DevicesSchemaScanNode", e);
    }
    int limit = ReadWriteIOUtils.readInt(byteBuffer);
    int offset = ReadWriteIOUtils.readInt(byteBuffer);
    boolean isPrefixPath = ReadWriteIOUtils.readBool(byteBuffer);
    boolean hasSgCol = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DevicesSchemaScanNode(planNodeId, path, limit, offset, isPrefixPath, hasSgCol);
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
    DevicesSchemaScanNode that = (DevicesSchemaScanNode) o;
    return hasSgCol == that.hasSgCol;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), hasSgCol);
  }
}
