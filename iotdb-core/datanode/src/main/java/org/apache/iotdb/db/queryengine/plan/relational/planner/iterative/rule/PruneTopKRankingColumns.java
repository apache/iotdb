package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import com.google.common.collect.Streams;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.topNRanking;

public class PruneTopKRankingColumns
    extends ProjectOffPushDownRule<TopKRankingNode>
{
  public PruneTopKRankingColumns()
  {
    super(topNRanking());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(Context context, TopKRankingNode topNRankingNode, Set<Symbol> referencedOutputs)
  {
    Set<Symbol> requiredInputs = Streams.concat(
            referencedOutputs.stream()
                .filter(symbol -> !symbol.equals(topNRankingNode.getRankingSymbol())),
            topNRankingNode.getSpecification().getPartitionBy().stream(),
            topNRankingNode.getSpecification().getOrderingScheme().get().getOrderBy().stream())
        .collect(toImmutableSet());

    return restrictChildOutputs(context.getIdAllocator(), topNRankingNode, requiredInputs);
  }
}
