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

public class RemoveRegion extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RemoveRegion.class);

  private final List<Integer> regionIds;
  private final int dataNodeId;

  public RemoveRegion(List<Integer> regionId, int dataNodeId) {
    super(null);
    this.regionIds = regionId;
    this.dataNodeId = dataNodeId;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(RemoveRegion.class, regionIds, dataNodeId);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RemoveRegion)) {
      return false;
    }
    RemoveRegion another = (RemoveRegion) obj;
    return regionIds.equals(another.regionIds) && dataNodeId == another.dataNodeId;
  }

  @Override
  public String toString() {
    return String.format("remove region %s from %d", regionIds, dataNodeId);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRemoveRegion(this, context);
  }

  public List<Integer> getRegionIds() {
    return regionIds;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfIntegerList(regionIds);
    return size;
  }
}
