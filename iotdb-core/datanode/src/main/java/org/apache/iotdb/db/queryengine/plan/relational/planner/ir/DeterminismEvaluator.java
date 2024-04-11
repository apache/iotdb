package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;

import java.util.concurrent.atomic.AtomicBoolean;

public final class DeterminismEvaluator {
  private DeterminismEvaluator() {}

  public static boolean isDeterministic(Expression expression) {
    AtomicBoolean deterministic = new AtomicBoolean(true);
    new Visitor().process(expression, deterministic);
    return deterministic.get();
  }

  private static class Visitor extends DefaultTraversalVisitor<AtomicBoolean> {
    @Override
    protected Void visitFunctionCall(FunctionCall node, AtomicBoolean deterministic) {
      //            if (!node.getFunction().isDeterministic()) {
      //                deterministic.set(false);
      //                return null;
      //            }
      return super.visitFunctionCall(node, deterministic);
    }
  }
}
