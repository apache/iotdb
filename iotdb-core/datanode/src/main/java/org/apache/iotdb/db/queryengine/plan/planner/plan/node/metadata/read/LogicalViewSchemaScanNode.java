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
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
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

public class LogicalViewSchemaScanNode extends SchemaQueryScanNode {

  private final SchemaFilter schemaFilter;

  public LogicalViewSchemaScanNode(
      PlanNodeId id,
      PartialPath partialPath,
      SchemaFilter schemaFilter,
      long limit,
      long offset,
      PathPatternTree scope) {
    super(id, partialPath, limit, offset, false, scope);
    this.schemaFilter = schemaFilter;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LOGICAL_VIEW_SCHEMA_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    scope.serialize(byteBuffer);
    SchemaFilter.serialize(schemaFilter, byteBuffer);
    ReadWriteIOUtils.write(limit, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LOGICAL_VIEW_SCHEMA_SCAN.serialize(stream);
    ReadWriteIOUtils.write(path.getFullPath(), stream);
    scope.serialize(stream);
    SchemaFilter.serialize(schemaFilter, stream);
    ReadWriteIOUtils.write(limit, stream);
    ReadWriteIOUtils.write(offset, stream);
  }

  public static LogicalViewSchemaScanNode deserialize(ByteBuffer byteBuffer) {
    String fullPath = ReadWriteIOUtils.readString(byteBuffer);
    PartialPath path;
    try {
      path = new PartialPath(fullPath);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize TimeSeriesSchemaScanNode", e);
    }
    PathPatternTree scope = PathPatternTree.deserialize(byteBuffer);
    SchemaFilter schemaFilter = SchemaFilter.deserialize(byteBuffer);
    long limit = ReadWriteIOUtils.readLong(byteBuffer);
    long offset = ReadWriteIOUtils.readLong(byteBuffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    return new LogicalViewSchemaScanNode(planNodeId, path, schemaFilter, limit, offset, scope);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.LOGICAL_VIEW_SCHEMA_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new LogicalViewSchemaScanNode(getPlanNodeId(), path, schemaFilter, limit, offset, scope);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ColumnHeaderConstant.showLogicalViewColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
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
    LogicalViewSchemaScanNode that = (LogicalViewSchemaScanNode) o;
    return Objects.equals(schemaFilter, that.schemaFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), schemaFilter);
  }

  @Override
  public String toString() {
    return String.format(
        "LogicalViewSchemaScanNode-%s:[DataRegion: %s]",
        this.getPlanNodeId(), this.getRegionReplicaSet());
  }
}
