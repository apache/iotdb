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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;

public class ReconstructRegion extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ReconstructRegion.class);
  final int dataNodeId;
  final List<Integer> regionIds;

  public ReconstructRegion(int dataNodeId, List<Integer> regionIds) {
    super(null);
    this.dataNodeId = dataNodeId;
    this.regionIds = regionIds;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(ReconstructRegion.class, dataNodeId, regionIds);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ReconstructRegion)) {
      return false;
    }
    ReconstructRegion another = (ReconstructRegion) obj;
    return dataNodeId == another.dataNodeId && regionIds.equals(another.regionIds);
  }

  @Override
  public String toString() {
    return String.format("reconstruct region %s of datanode %d", regionIds, dataNodeId);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitReconstructRegion(this, context);
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  public List<Integer> getRegionIds() {
    return regionIds;
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.shallowSizeOf(regionIds);
    return size;
  }
}
