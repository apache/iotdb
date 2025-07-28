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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.ChildReplacer.replaceChildren;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.aggregation;

public class PruneDistinctAggregation implements Rule<AggregationNode> {
  private static final Pattern<AggregationNode> PATTERN =
      aggregation().matching(PruneDistinctAggregation::isDistinctOperator);

  @Override
  public Pattern<AggregationNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(AggregationNode node, Captures captures, Context context) {
    Lookup lookup = context.getLookup();
    DistinctAggregationRewriter rewriter = new DistinctAggregationRewriter(lookup);

    List<PlanNode> newSources =
        node.getChildren().stream()
            .map(lookup::resolve)
            .map(source -> source.accept(rewriter, true))
            .collect(toImmutableList());

    if (rewriter.isRewritten()) {
      return Result.ofPlanNode(replaceChildren(node, newSources));
    }
    return Result.empty();
  }

  private static boolean isDistinctOperator(AggregationNode node) {
    return node.getAggregations().isEmpty();
  }

  private static class DistinctAggregationRewriter extends PlanVisitor<PlanNode, Boolean> {
    private final Lookup lookup;
    private boolean rewritten;

    public DistinctAggregationRewriter(Lookup lookup) {
      this.lookup = lookup;
      this.rewritten = false;
    }

    public boolean isRewritten() {
      return rewritten;
    }

    private PlanNode rewriteChildren(PlanNode node, Boolean context) {
      List<PlanNode> newSources =
          node.getChildren().stream()
              .map(lookup::resolve)
              .map(source -> source.accept(this, context))
              .collect(toImmutableList());

      return replaceChildren(node, newSources);
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Boolean context) {
      // Unable to remove distinct aggregation anymore.
      return rewriteChildren(node, false);
    }

    /*@Override
    public PlanNode visitUnion(UnionNode node, Boolean context)
    {
        return rewriteChildren(node, context);
    }

    @Override
    public PlanNode visitIntersect(IntersectNode node, Boolean context)
    {
        if (node.isDistinct()) {
            return rewriteChildren(node, context);
        }
        return visitPlan(node, context);
    }

    @Override
    public PlanNode visitExcept(ExceptNode node, Boolean context)
    {
        if (node.isDistinct()) {
            return rewriteChildren(node, context);
        }
        return visitPlan(node, context);
    }*/

    @Override
    public PlanNode visitAggregation(AggregationNode node, Boolean context) {
      boolean distinct = isDistinctOperator(node);

      PlanNode rewrittenNode = lookup.resolve(node.getChild()).accept(this, distinct);

      if (context && distinct) {
        this.rewritten = true;
        // Assumes underlying node has same output symbols as this distinct node
        return rewrittenNode;
      }

      return AggregationNode.builderFrom(node)
          .setSource(rewrittenNode)
          .setPreGroupedSymbols(ImmutableList.of())
          .build();
    }
  }
}
