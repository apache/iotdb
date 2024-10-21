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
import org.apache.iotdb.commons.path.PartialPath;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowRegions extends Statement {

  private final TConsensusGroupType regionType;
  private final List<PartialPath> databases;
  private final List<Integer> nodeIds;

  public ShowRegions(
      TConsensusGroupType regionType, List<PartialPath> databases, List<Integer> nodeIds) {
    super(null);
    this.regionType = regionType;
    this.databases = databases;
    this.nodeIds = nodeIds;
  }

  public ShowRegions(
      NodeLocation location,
      TConsensusGroupType regionType,
      List<PartialPath> storageGroups,
      List<Integer> nodeIds) {
    super(requireNonNull(location, "location is null"));
    this.regionType = regionType;
    this.databases = requireNonNull(storageGroups, "databases is null");
    this.nodeIds = requireNonNull(nodeIds, "nodeIds is null");
  }

  public TConsensusGroupType getRegionType() {
    return regionType;
  }

  public List<PartialPath> getDatabases() {
    return databases;
  }

  public List<Integer> getNodeIds() {
    return nodeIds;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowRegions(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShowRegions that = (ShowRegions) o;
    return regionType == that.regionType
        && Objects.equals(databases, that.databases)
        && Objects.equals(nodeIds, that.nodeIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionType, databases, nodeIds);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("regionType", regionType)
        .add("databases", databases)
        .add("nodeIds", nodeIds)
        .toString();
  }
}
