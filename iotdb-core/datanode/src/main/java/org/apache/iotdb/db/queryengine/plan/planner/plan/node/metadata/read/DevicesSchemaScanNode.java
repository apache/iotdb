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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DevicesSchemaScanNode extends SchemaQueryScanNode {

  private final boolean hasSgCol;
  private final SchemaFilter schemaFilter;

  public DevicesSchemaScanNode(
      PlanNodeId id,
      PartialPath path,
      long limit,
      long offset,
      boolean isPrefixPath,
      boolean hasSgCol,
      SchemaFilter schemaFilter,
      PathPatternTree scope) {
    super(id, path, limit, offset, isPrefixPath, scope);
    this.hasSgCol = hasSgCol;
    this.schemaFilter = schemaFilter;
  }

  public boolean isHasSgCol() {
    return hasSgCol;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.DEVICES_SCHEMA_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new DevicesSchemaScanNode(
        getPlanNodeId(), path, limit, offset, isPrefixPath, hasSgCol, schemaFilter, scope);
  }

  @Override
  public List<String> getOutputColumnNames() {
    if (hasSgCol) {
      return ColumnHeaderConstant.showDevicesWithSgColumnHeaders.stream()
          .map(ColumnHeader::getColumnName)
          .collect(Collectors.toList());
    }
    return ColumnHeaderConstant.showDevicesColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICES_SCHEMA_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    scope.serialize(byteBuffer);
    ReadWriteIOUtils.write(limit, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
    ReadWriteIOUtils.write(isPrefixPath, byteBuffer);
    ReadWriteIOUtils.write(hasSgCol, byteBuffer);
    SchemaFilter.serialize(schemaFilter, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICES_SCHEMA_SCAN.serialize(stream);
    ReadWriteIOUtils.write(path.getFullPath(), stream);
    scope.serialize(stream);
    ReadWriteIOUtils.write(limit, stream);
    ReadWriteIOUtils.write(offset, stream);
    ReadWriteIOUtils.write(isPrefixPath, stream);
    ReadWriteIOUtils.write(hasSgCol, stream);
    SchemaFilter.serialize(schemaFilter, stream);
  }

  public static DevicesSchemaScanNode deserialize(ByteBuffer byteBuffer) {
    String fullPath = ReadWriteIOUtils.readString(byteBuffer);
    PartialPath path;
    try {
      path = new PartialPath(fullPath);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize DevicesSchemaScanNode", e);
    }
    PathPatternTree scope = PathPatternTree.deserialize(byteBuffer);
    long limit = ReadWriteIOUtils.readLong(byteBuffer);
    long offset = ReadWriteIOUtils.readLong(byteBuffer);
    boolean isPrefixPath = ReadWriteIOUtils.readBool(byteBuffer);
    boolean hasSgCol = ReadWriteIOUtils.readBool(byteBuffer);
    SchemaFilter schemaFilter = SchemaFilter.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DevicesSchemaScanNode(
        planNodeId, path, limit, offset, isPrefixPath, hasSgCol, schemaFilter, scope);
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
    return hasSgCol == that.hasSgCol && Objects.equals(schemaFilter, that.schemaFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), hasSgCol, schemaFilter);
  }

  @Override
  public String toString() {
    return String.format("DevicesSchemaScanNode-%s[Path: %s]", getPlanNodeId(), path);
  }
}
