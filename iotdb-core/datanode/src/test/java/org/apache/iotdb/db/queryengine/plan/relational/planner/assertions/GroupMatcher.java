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

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;

public class GroupMatcher extends SortMatcher {
  private final int partitionKeyCount;

  public GroupMatcher(List<PlanMatchPattern.Ordering> orderBy, int partitionKeyCount) {
    super(orderBy);
    this.partitionKeyCount = partitionKeyCount;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof GroupNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    MatchResult result = super.detailMatches(node, sessionInfo, metadata, symbolAliases);
    if (result != NO_MATCH) {
      GroupNode sortNode = (GroupNode) node;
      if (partitionKeyCount != ((GroupNode) node).getPartitionKeyCount()) {
        return NO_MATCH;
      }
      return MatchResult.match();
    }
    return NO_MATCH;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("orderBy", orderBy)
        .add("partitionKeyCount", partitionKeyCount)
        .toString();
  }
}
