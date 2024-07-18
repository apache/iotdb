package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

import com.google.common.collect.Streams;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.topK;

public class PruneTopKColumns extends ProjectOffPushDownRule<TopKNode> {
  public PruneTopKColumns() {
    super(topK());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, TopKNode topKNode, Set<Symbol> referencedOutputs) {
    Set<Symbol> prunedTopNInputs =
        Streams.concat(
                referencedOutputs.stream(), topKNode.getOrderingScheme().getOrderBy().stream())
            .collect(toImmutableSet());

    return restrictChildOutputs(context.getIdAllocator(), topKNode, prunedTopNInputs);
  }
}
