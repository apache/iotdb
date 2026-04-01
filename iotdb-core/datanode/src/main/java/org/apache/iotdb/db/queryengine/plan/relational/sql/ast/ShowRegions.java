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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ShowRegions extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ShowRegions.class);

  private final TConsensusGroupType regionType;
  private final String database;
  private final List<Integer> nodeIds;

  public ShowRegions(
      final TConsensusGroupType regionType, final String database, final List<Integer> nodeIds) {
    super(null);
    this.regionType = regionType;
    this.database = database;
    this.nodeIds = nodeIds;
  }

  public TConsensusGroupType getRegionType() {
    return regionType;
  }

  public String getDatabase() {
    return database;
  }

  public List<Integer> getNodeIds() {
    return nodeIds;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowRegions(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ShowRegions that = (ShowRegions) o;
    return regionType == that.regionType
        && Objects.equals(database, that.database)
        && Objects.equals(nodeIds, that.nodeIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionType, database, nodeIds);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("regionType", regionType)
        .add("database", database)
        .add("nodeIds", nodeIds)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(database);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfIntegerList(nodeIds);
    return size;
  }
}
