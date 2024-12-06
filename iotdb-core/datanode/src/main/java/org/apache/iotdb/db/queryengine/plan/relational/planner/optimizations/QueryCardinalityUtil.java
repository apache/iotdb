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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.GroupReference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

import com.google.common.collect.Range;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup.noLookup;

public final class QueryCardinalityUtil {
  private QueryCardinalityUtil() {}

  public static boolean isScalar(PlanNode node) {
    return isScalar(node, noLookup());
  }

  public static boolean isScalar(PlanNode node, Lookup lookup) {
    return extractCardinality(node, lookup).isScalar();
  }

  public static boolean isAtMostScalar(PlanNode node) {
    return isAtMostScalar(node, noLookup());
  }

  public static boolean isAtMostScalar(PlanNode node, Lookup lookup) {
    return isAtMost(node, lookup, 1L);
  }

  public static boolean isAtMost(PlanNode node, Lookup lookup, long maxCardinality) {
    return extractCardinality(node, lookup).isAtMost(maxCardinality);
  }

  public static boolean isAtLeastScalar(PlanNode node, Lookup lookup) {
    return isAtLeast(node, lookup, 1L);
  }

  public static boolean isAtLeast(PlanNode node, Lookup lookup, long minCardinality) {
    return extractCardinality(node, lookup).isAtLeast(minCardinality);
  }

  public static boolean isEmpty(PlanNode node, Lookup lookup) {
    return isAtMost(node, lookup, 0);
  }

  public static Cardinality extractCardinality(PlanNode node) {
    return extractCardinality(node, noLookup());
  }

  public static Cardinality extractCardinality(PlanNode node, Lookup lookup) {
    return new Cardinality(node.accept(new CardinalityExtractorPlanVisitor(lookup), null));
  }

  private static final class CardinalityExtractorPlanVisitor
      extends PlanVisitor<Range<Long>, Void> {
    private final Lookup lookup;

    public CardinalityExtractorPlanVisitor(Lookup lookup) {
      this.lookup = requireNonNull(lookup, "lookup is null");
    }

    @Override
    public Range<Long> visitPlan(PlanNode node, Void context) {
      return Range.atLeast(0L);
    }

    @Override
    public Range<Long> visitGroupReference(GroupReference node, Void context) {
      return lookup.resolve(node).accept(this, context);
    }

    @Override
    public Range<Long> visitEnforceSingleRow(EnforceSingleRowNode node, Void context) {
      return Range.singleton(1L);
    }

    @Override
    public Range<Long> visitAggregation(AggregationNode node, Void context) {
      if (node.hasSingleGlobalAggregation()) {
        // only single default aggregation which will produce exactly single row
        return Range.singleton(1L);
      }

      Range<Long> sourceCardinalityRange = node.getChild().accept(this, null);

      long lower;
      if (node.hasDefaultOutput() || sourceCardinalityRange.lowerEndpoint() > 0) {
        lower = 1;
      } else {
        lower = 0;
      }

      if (sourceCardinalityRange.hasUpperBound()) {
        long upper = Math.max(lower, sourceCardinalityRange.upperEndpoint());
        return Range.closed(lower, upper);
      }

      return Range.atLeast(lower);
    }

    @Override
    public Range<Long> visitExchange(ExchangeNode node, Void context) {
      if (node.getChildren().size() == 1) {
        return getOnlyElement(node.getChildren()).accept(this, null);
      }
      return Range.atLeast(0L);
    }

    @Override
    public Range<Long> visitProject(ProjectNode node, Void context) {
      return node.getChild().accept(this, null);
    }

    @Override
    public Range<Long> visitFilter(FilterNode node, Void context) {
      Range<Long> sourceCardinalityRange = node.getChild().accept(this, null);
      if (sourceCardinalityRange.hasUpperBound()) {
        return Range.closed(0L, sourceCardinalityRange.upperEndpoint());
      }
      return Range.atLeast(0L);
    }

    //        @Override
    //        public Range<Long> visitValues(ValuesNode node, Void context)
    //        {
    //            return Range.singleton((long) node.getRowCount());
    //        }

    @Override
    public Range<Long> visitOffset(OffsetNode node, Void context) {
      Range<Long> sourceCardinalityRange = node.getChild().accept(this, null);

      long lower = max(sourceCardinalityRange.lowerEndpoint() - node.getCount(), 0L);

      if (sourceCardinalityRange.hasUpperBound()) {
        return Range.closed(
            lower, max(sourceCardinalityRange.upperEndpoint() - node.getCount(), 0L));
      }
      return Range.atLeast(lower);
    }

    @Override
    public Range<Long> visitLimit(LimitNode node, Void context) {
      if (node.isWithTies()) {
        Range<Long> sourceCardinalityRange = node.getChild().accept(this, null);
        long lower = min(node.getCount(), sourceCardinalityRange.lowerEndpoint());
        if (sourceCardinalityRange.hasUpperBound()) {
          return Range.closed(lower, sourceCardinalityRange.upperEndpoint());
        }
        return Range.atLeast(lower);
      }

      return applyLimit(node.getChild(), node.getCount());
    }

    @Override
    public Range<Long> visitTopK(TopKNode node, Void context) {
      long limit = node.getCount();
      List<Range<Long>> rangeList =
          node.getChildren().stream()
              .map(child -> applyLimit(child, limit))
              .collect(Collectors.toList());
      // merge rangeList
      long lower = rangeList.stream().mapToLong(Range::lowerEndpoint).sum();
      long upper = rangeList.stream().mapToLong(Range::upperEndpoint).sum();
      return Range.closed(Math.min(lower, limit), Math.min(upper, limit));
    }

    //        @Override
    //        public Range<Long> visitWindow(WindowNode node, Void context)
    //        {
    //            return node.getSource().accept(this, null);
    //        }

    private Range<Long> applyLimit(PlanNode source, long limit) {
      Range<Long> sourceCardinalityRange = source.accept(this, null);
      if (sourceCardinalityRange.hasUpperBound()) {
        limit = min(sourceCardinalityRange.upperEndpoint(), limit);
      }
      long lower = min(limit, sourceCardinalityRange.lowerEndpoint());
      return Range.closed(lower, limit);
    }
  }
}
