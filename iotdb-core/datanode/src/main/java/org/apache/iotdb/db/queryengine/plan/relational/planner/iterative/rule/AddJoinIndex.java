package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.join;

/** <b>Optimization phase:</b> Distributed plan planning. */
public class AddJoinIndex implements Rule<JoinNode> {

  private static final Pattern<JoinNode> PATTERN = join();

  @Override
  public Pattern<JoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(JoinNode node, Captures captures, Context context) {
    node.leftTimeColumnIdx =
        node.getLeftChild().getOutputSymbols().indexOf(node.getCriteria().get(0).getLeft());
    node.rightTimeColumnIdx =
        node.getRightChild().getOutputSymbols().indexOf(node.getCriteria().get(0).getRight());

    node.leftOutputSymbolIdx = new int[node.getLeftOutputSymbols().size()];
    for (int i = 0; i < node.leftOutputSymbolIdx.length; i++) {
      node.leftOutputSymbolIdx[i] =
          node.getLeftChild().getOutputSymbols().indexOf(node.getLeftOutputSymbols().get(i));
    }
    node.rightOutputSymbolIdx = new int[node.getRightOutputSymbols().size()];
    for (int i = 0; i < node.rightOutputSymbolIdx.length; i++) {
      node.rightOutputSymbolIdx[i] =
          node.getRightChild().getOutputSymbols().indexOf(node.getRightOutputSymbols().get(i));
    }
    return Result.empty();
  }
}
