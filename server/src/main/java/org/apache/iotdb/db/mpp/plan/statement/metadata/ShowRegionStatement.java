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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.List;

public class ShowRegionStatement extends ShowStatement implements IConfigStatement {

  private TConsensusGroupType regionType;
  private List<PartialPath> storageGroups;

  private List<Integer> nodeIds;

  public ShowRegionStatement() {}

  public ShowRegionStatement(TConsensusGroupType regionType) {
    this.regionType = regionType;
  }

  public ShowRegionStatement(
      TConsensusGroupType regionType, List<PartialPath> storageGroups, List<Integer> nodeIds) {
    this.regionType = regionType;
    this.storageGroups = storageGroups;
    this.nodeIds = nodeIds;
  }

  public List<PartialPath> getStorageGroups() {
    return storageGroups;
  }

  public void setStorageGroups(List<PartialPath> storageGroups) {
    this.storageGroups = storageGroups;
  }

  public TConsensusGroupType getRegionType() {
    return regionType;
  }

  public void setRegionType(TConsensusGroupType regionType) {
    this.regionType = regionType;
  }

  public void setNodeIds(List<Integer> nodeIds) {
    this.nodeIds = nodeIds;
  }

  public List<Integer> getNodeIds() {
    return nodeIds;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowRegion(this, context);
  }
}
