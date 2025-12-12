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
import java.util.Objects;

public class MigrateRegion extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MigrateRegion.class);
  private final int regionId;

  private final int fromId;

  private final int toId;

  public MigrateRegion(int regionId, int fromId, int toId) {
    this(null, regionId, fromId, toId);
  }

  public MigrateRegion(@Nullable NodeLocation location, int regionId, int fromId, int toId) {
    super(location);
    this.regionId = regionId;
    this.fromId = fromId;
    this.toId = toId;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(MigrateRegion.class, regionId, fromId, toId);
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
    return regionId == another.regionId && fromId == another.fromId && toId == another.toId;
  }

  @Override
  public String toString() {
    return String.format("migrate region %d from %d to %d", regionId, fromId, toId);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitMigrateRegion(this, context);
  }

  public int getRegionId() {
    return regionId;
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
    return size;
  }
}
