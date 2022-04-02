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

import org.apache.iotdb.commons.partition.SchemaRegionReplicaSet;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;

import java.nio.ByteBuffer;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;

public abstract class MetaScanNode extends PlanNode {
  protected int limit = 0;
  protected int offset = 0;
  protected PartialPath path;
  private boolean hasLimit;
  private boolean isPrefixPath;

  private SchemaRegionReplicaSet schemaRegionReplicaSet;

  protected MetaScanNode(PlanNodeId id, PartialPath partialPath, int limit, int offset, boolean isPrefixPath) {
    super(id);
    this.path = partialPath;
    setLimit(limit);
    this.offset = offset;
    this.isPrefixPath = isPrefixPath;
  }

  public boolean isPrefixPath() {
    return isPrefixPath;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
    if (limit == 0) {
      hasLimit = false;
    } else {
      hasLimit = true;
    }
  }

  public SchemaRegionReplicaSet getSchemaRegionReplicaSet() {
    return schemaRegionReplicaSet;
  }

  public void setSchemaRegionReplicaSet(
      SchemaRegionReplicaSet schemaRegionReplicaSet) {
    this.schemaRegionReplicaSet = schemaRegionReplicaSet;
  }

  public int getOffset() {
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

  public boolean isHasLimit() {
    return hasLimit;
  }

  public void setHasLimit(boolean hasLimit) {
    this.hasLimit = hasLimit;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitMetaScan(this, context);
  }
}
