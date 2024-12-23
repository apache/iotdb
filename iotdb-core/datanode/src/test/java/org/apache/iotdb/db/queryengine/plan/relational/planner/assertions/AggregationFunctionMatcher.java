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

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AggregationFunctionMatcher implements RvalueMatcher {
  private final ExpectedValueProvider<AggregationFunction> callMaker;

  public AggregationFunctionMatcher(ExpectedValueProvider<AggregationFunction> callMaker) {
    this.callMaker = requireNonNull(callMaker, "callMaker is null");
  }

  @Override
  public Optional<Symbol> getAssignedSymbol(
      PlanNode node, SessionInfo session, Metadata metadata, SymbolAliases symbolAliases) {
    Optional<Symbol> result = Optional.empty();
    if (!(node instanceof AggregationNode)) {
      return result;
    }

    AggregationNode aggregationNode = (AggregationNode) node;
    AggregationFunction expectedCall = callMaker.getExpectedValue(symbolAliases);
    for (Map.Entry<Symbol, AggregationNode.Aggregation> assignment :
        aggregationNode.getAggregations().entrySet()) {
      AggregationNode.Aggregation aggregation = assignment.getValue();
      if (aggregationMatches(aggregation, expectedCall)) {
        checkState(!result.isPresent(), "Ambiguous function calls in %s", aggregationNode);
        result = Optional.of(assignment.getKey());
      }
    }

    return result;
  }

  private static boolean aggregationMatches(
      AggregationNode.Aggregation aggregation, AggregationFunction expectedCall) {
    return Objects.equals(
            expectedCall.getName(), aggregation.getResolvedFunction().getSignature().getName())
        && Objects.equals(expectedCall.getFilter(), aggregation.getFilter())
        && Objects.equals(expectedCall.getOrderBy(), aggregation.getOrderingScheme())
        && Objects.equals(expectedCall.isDistinct(), aggregation.isDistinct())
        && Objects.equals(expectedCall.getArguments(), aggregation.getArguments());
  }

  @Override
  public String toString() {
    return callMaker.toString();
  }
}
