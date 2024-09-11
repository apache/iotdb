/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.Streams;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.aggregation;

public class PruneAggregationSourceColumns implements Rule<AggregationNode> {
  private static final Pattern<AggregationNode> PATTERN = aggregation();

  @Override
  public Pattern<AggregationNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(AggregationNode aggregationNode, Captures captures, Context context) {
    Set<Symbol> requiredInputs =
        Streams.concat(
                aggregationNode.getGroupingKeys().stream(),
                aggregationNode.getHashSymbol().map(Stream::of).orElseGet(Stream::empty),
                aggregationNode.getAggregations().values().stream()
                    .flatMap(aggregation -> SymbolsExtractor.extractUnique(aggregation).stream()))
            .collect(toImmutableSet());

    return restrictChildOutputs(context.getIdAllocator(), aggregationNode, requiredInputs)
        .map(Result::ofPlanNode)
        .orElse(Result.empty());
  }
}
