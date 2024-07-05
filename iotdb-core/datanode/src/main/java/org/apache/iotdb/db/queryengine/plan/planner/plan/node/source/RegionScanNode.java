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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.source;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import java.util.Objects;
import java.util.Set;

public abstract class RegionScanNode extends SourceNode {

  protected TRegionReplicaSet regionReplicaSet;
  protected boolean outputCount = false;

  protected RegionScanNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public boolean isOutputCount() {
    return outputCount;
  }

  public void setOutputCount(boolean outputCount) {
    this.outputCount = outputCount;
  }

  public abstract Set<PartialPath> getDevicePaths();

  public abstract void addDevicePath(PartialPath devicePath, RegionScanNode node);

  public abstract void clearPath();

  public abstract long getSize();

  @Override
  public void open() throws Exception {}

  @Override
  public void close() throws Exception {}

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
    RegionScanNode that = (RegionScanNode) o;
    return Objects.equals(regionReplicaSet, that.regionReplicaSet)
        && Objects.equals(outputCount, that.outputCount);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitRegionScan(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), regionReplicaSet, outputCount);
  }
}
