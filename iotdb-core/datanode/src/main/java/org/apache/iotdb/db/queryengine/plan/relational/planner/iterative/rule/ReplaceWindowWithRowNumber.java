package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;

public class ReplaceWindowWithRowNumber implements Rule<WindowNode> {
  private final Pattern<WindowNode> pattern;

  public ReplaceWindowWithRowNumber(Metadata metadata) {
    this.pattern =
        window()
            .matching(
                window -> {
                  if (window.getWindowFunctions().size() != 1) {
                    return false;
                  }
                  BoundSignature signature =
                      getOnlyElement(window.getWindowFunctions().values())
                          .getResolvedFunction()
                          .getSignature();
                  return signature.getArgumentTypes().isEmpty()
                      && signature.getName().equals("row_number");
                })
            .matching(window -> !window.getSpecification().getOrderingScheme().isPresent());
  }

  @Override
  public Pattern<WindowNode> getPattern() {
    return pattern;
  }

  @Override
  public Result apply(WindowNode node, Captures captures, Context context) {
    return null;
  }
}
