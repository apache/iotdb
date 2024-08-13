package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;

import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.join;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.MoreLists.filteredCopy;

/** Joins support output symbol selection, so absorb any project-off into the node. */
public class PruneJoinColumns extends ProjectOffPushDownRule<JoinNode> {
  public PruneJoinColumns() {
    super(join());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, JoinNode joinNode, Set<Symbol> referencedOutputs) {
    return Optional.of(
        new JoinNode(
            joinNode.getPlanNodeId(),
            joinNode.getJoinType(),
            joinNode.getLeftChild(),
            joinNode.getRightChild(),
            joinNode.getCriteria(),
            filteredCopy(joinNode.getLeftOutputSymbols(), referencedOutputs::contains),
            filteredCopy(joinNode.getRightOutputSymbols(), referencedOutputs::contains),
            joinNode.isMaySkipOutputDuplicates(),
            joinNode.getFilter(),
            joinNode.getLeftHashSymbol(),
            joinNode.getRightHashSymbol(),
            joinNode.isSpillable()));
  }
}
