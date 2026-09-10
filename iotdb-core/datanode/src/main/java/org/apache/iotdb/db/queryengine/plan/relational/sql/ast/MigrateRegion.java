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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class MigrateRegion extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MigrateRegion.class);
  private final List<Integer> regionIds;

  private final int fromId;

  private final int toId;

  public MigrateRegion(List<Integer> regionIds, int fromId, int toId) {
    this(null, regionIds, fromId, toId);
  }

  public MigrateRegion(
      @Nullable NodeLocation location, List<Integer> regionIds, int fromId, int toId) {
    super(location);
    this.regionIds = regionIds;
    this.fromId = fromId;
    this.toId = toId;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(MigrateRegion.class, regionIds, fromId, toId);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MigrateRegion)) {
      return false;
    }
    MigrateRegion another = (MigrateRegion) obj;
    return regionIds.equals(another.regionIds) && fromId == another.fromId && toId == another.toId;
  }

  @Override
  public String toString() {
    return String.format("migrate region %s from %d to %d", regionIds, fromId, toId);
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((AstVisitor<R, C>) visitor).visitMigrateRegion(this, context);
  }

  public List<Integer> getRegionIds() {
    return regionIds;
  }

  public int getFromId() {
    return fromId;
  }

  public int getToId() {
    return toId;
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfIntegerList(regionIds);
    return size;
  }
}
