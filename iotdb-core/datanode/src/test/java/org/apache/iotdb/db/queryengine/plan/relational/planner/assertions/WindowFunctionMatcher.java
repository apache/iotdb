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
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.Util.orderingSchemeMatches;

public class WindowFunctionMatcher implements Matcher {
  private final PlanMatchPattern.Specification specification;

  public WindowFunctionMatcher(PlanMatchPattern.Specification specification) {
    this.specification = specification;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof WindowNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());
    WindowNode windowNode = (WindowNode) node;

    Optional<OrderingScheme> orderingScheme = windowNode.getSpecification().getOrderingScheme();
    if (orderingScheme.isPresent()) {
      if (!orderingSchemeMatches(
          specification.getOrdering(), orderingScheme.get(), symbolAliases)) {
        return NO_MATCH;
      }
    } else if (!specification.getOrdering().isEmpty()) {
      return NO_MATCH;
    }

    List<Symbol> partitionBy = windowNode.getSpecification().getPartitionBy();
    if (partitionBy.size() != specification.getPartitionKeys().size()) {
      return NO_MATCH;
    } else {
      for (int i = 0; i < specification.getPartitionKeys().size(); i++) {
        if (!Objects.equals(
            partitionBy.get(i).toString(), specification.getPartitionKeys().get(i))) {
          return NO_MATCH;
        }
      }
    }

    return MatchResult.match();
  }
}
