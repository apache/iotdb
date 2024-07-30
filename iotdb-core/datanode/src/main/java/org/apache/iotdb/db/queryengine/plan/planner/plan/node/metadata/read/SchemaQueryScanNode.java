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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class SchemaQueryScanNode extends SourceNode {
  protected long limit;
  protected long offset;
  protected PartialPath path;
  protected PathPatternTree scope;
  private boolean hasLimit;
  protected boolean isPrefixPath;

  private TRegionReplicaSet schemaRegionReplicaSet;

  protected SchemaQueryScanNode(PlanNodeId id) {
    this(id, null, false, SchemaConstant.ALL_MATCH_SCOPE);
  }

  protected SchemaQueryScanNode(
      PlanNodeId id,
      PartialPath partialPath,
      long limit,
      long offset,
      boolean isPrefixPath,
      PathPatternTree scope) {
    super(id);
    this.path = partialPath;
    this.scope = scope;
    setLimit(limit);
    this.offset = offset;
    this.isPrefixPath = isPrefixPath;
  }

  protected SchemaQueryScanNode(
      PlanNodeId id, PartialPath partialPath, boolean isPrefixPath, PathPatternTree scope) {
    this(id, partialPath, 0, 0, isPrefixPath, scope);
  }

  @Override
  public void open() throws Exception {}

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public void close() throws Exception {}

  public List<PartialPath> getPathPatternList() {
    return Collections.singletonList(path);
  }

  public void setPathPatternList(List<PartialPath> pathPatternList) {
    if (pathPatternList.size() == 1) {
      this.path = pathPatternList.get(0);
    }
  }

  public boolean isPrefixPath() {
    return isPrefixPath;
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(long limit) {
    this.limit = limit;
    if (limit == 0) {
      hasLimit = false;
    } else {
      hasLimit = true;
    }
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return schemaRegionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet schemaRegionReplicaSet) {
    this.schemaRegionReplicaSet = schemaRegionReplicaSet;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public PathPatternTree getScope() {
    return scope;
  }

  public void setScope(PathPatternTree scope) {
    this.scope = scope;
  }

  public boolean isHasLimit() {
    return hasLimit;
  }

  public void setHasLimit(boolean hasLimit) {
    this.hasLimit = hasLimit;
  }

  @Override
  public String toString() {
    return String.format(
        "SchemaQueryScanNode-%s:[Path: %s, DataRegion: %s]",
        this.getPlanNodeId(), path, PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSchemaQueryScan(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SchemaQueryScanNode that = (SchemaQueryScanNode) o;
    return limit == that.limit
        && offset == that.offset
        && isPrefixPath == that.isPrefixPath
        && Objects.equals(path, that.path)
        && Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), limit, offset, path, scope, isPrefixPath);
  }
}
