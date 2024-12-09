/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.gson.internal.$Gson$Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;

public class InformationSchemaTableScanMatcher extends TableScanMatcher {
  private final Optional<Integer> dataNodeId;

  public InformationSchemaTableScanMatcher(
      String expectedTableName,
      Optional<Boolean> hasTableLayout,
      List<String> outputSymbols,
      Set<String> assignmentsKeys,
      Optional<Integer> dataNodeId) {
    super(expectedTableName, hasTableLayout, outputSymbols, assignmentsKeys);
    this.dataNodeId = dataNodeId;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof InformationSchemaTableScanNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    if (super.detailMatches(node, sessionInfo, metadata, symbolAliases) == NO_MATCH) {
      return NO_MATCH;
    }

    InformationSchemaTableScanNode tableScanNode = (InformationSchemaTableScanNode) node;

    TRegionReplicaSet regionReplicaSet = tableScanNode.getRegionReplicaSet();
    Integer actual = null;
    if (regionReplicaSet != null) {
      checkArgument(regionReplicaSet.getDataNodeLocations().size() == 1);
      actual = regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId();
    }
    if (!Objects.equals(actual, dataNodeId.orElse(null))) {
      return NO_MATCH;
    }

    return new MatchResult(true);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .omitNullValues()
        .add("expectedTableName", expectedTableName)
        .add("hasTableLayout", hasTableLayout.orElse(null))
        .add("outputSymbols", outputSymbols)
        .add("assignmentsKeys", assignmentsKeys)
        .add("dataNodeId", dataNodeId.orElse(null))
        .toString();
  }
}
