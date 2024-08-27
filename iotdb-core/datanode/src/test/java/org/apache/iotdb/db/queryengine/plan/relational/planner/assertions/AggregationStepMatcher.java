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
package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;

public class AggregationStepMatcher implements Matcher {
  private final AggregationNode.Step step;

  public AggregationStepMatcher(AggregationNode.Step step) {
    this.step = step;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof AggregationNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo session, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());
    AggregationNode aggregationNode = (AggregationNode) node;

    if (aggregationNode.getStep() != step) {
      return NO_MATCH;
    }
    return match();
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("step", step).toString();
  }
}
