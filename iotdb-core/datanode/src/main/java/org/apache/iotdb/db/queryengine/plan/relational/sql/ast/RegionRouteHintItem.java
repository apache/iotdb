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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RegionRouteHintItem extends Node {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RegionRouteHintItem.class);
  private static final String HINT_NAME_ITEM = "region_route";

  private @Nullable final QualifiedName table;
  private final Map<Integer, Integer> regionDatanodeMap;

  public RegionRouteHintItem(
      @Nullable QualifiedName table, Map<Integer, Integer> regionDatanodeMap) {
    super(null);
    this.table = table;
    this.regionDatanodeMap = regionDatanodeMap;
  }

  public @Nullable QualifiedName getTable() {
    return table;
  }

  public Map<Integer, Integer> getRegionDatanodeMap() {
    return regionDatanodeMap;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRegionRouteHintItem(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, regionDatanodeMap);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    RegionRouteHintItem other = (RegionRouteHintItem) obj;
    return this.table.equals(other.table) && regionDatanodeMap.equals(other.regionDatanodeMap);
  }

  @Override
  public String toString() {
    return HINT_NAME_ITEM + (table == null ? "" : "-" + table) + "(" + regionDatanodeMap + ")";
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(table)
        + RamUsageEstimator.sizeOfMap(regionDatanodeMap);
  }
}
