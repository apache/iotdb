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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class NodePathsSchemaScanNode extends SchemaQueryScanNode {
  // the path could be a prefix path with wildcard
  private PartialPath prefixPath;
  private int level = -1;

  public NodePathsSchemaScanNode(
      PlanNodeId id, PartialPath prefixPath, int level, PathPatternTree scope) {
    super(id);
    setScope(scope);
    this.prefixPath = prefixPath;
    this.level = level;
  }

  public PartialPath getPrefixPath() {
    return prefixPath;
  }

  public int getLevel() {
    return level;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.NODE_PATHS_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new NodePathsSchemaScanNode(getPlanNodeId(), prefixPath, level, scope);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ColumnHeaderConstant.showChildPathsColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.NODE_PATHS_SCAN.serialize(byteBuffer);
    prefixPath.serialize(byteBuffer);
    scope.serialize(byteBuffer);
    byteBuffer.putInt(level);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.NODE_PATHS_SCAN.serialize(stream);
    prefixPath.serialize(stream);
    scope.serialize(stream);
    stream.writeInt(level);
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    PartialPath path = (PartialPath) PathDeserializeUtil.deserialize(buffer);
    PathPatternTree scope = PathPatternTree.deserialize(buffer);
    int level = buffer.getInt();
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new NodePathsSchemaScanNode(planNodeId, path, level, scope);
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
    NodePathsSchemaScanNode that = (NodePathsSchemaScanNode) o;
    return level == that.level && Objects.equals(prefixPath, that.prefixPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), prefixPath, level);
  }

  @Override
  public String toString() {
    return String.format(
        "NodePathsSchemaScanNode-%s:[DataRegion: %s]",
        this.getPlanNodeId(), this.getRegionReplicaSet());
  }
}
