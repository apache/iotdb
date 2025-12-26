package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;

public class RemoveRedundantWindow implements Rule<WindowNode> {
  private static final Pattern<WindowNode> PATTERN = window();

  @Override
  public Pattern<WindowNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(WindowNode window, Captures captures, Context context) {
    //    if (isEmpty(window.getChild(), context.getLookup())) {
    //      return Result.ofPlanNode(new ValuesNode(window.getPlanNodeId(),
    // window.getOutputSymbols(), ImmutableList.of()));
    //    }
    return Result.empty();
  }
}
