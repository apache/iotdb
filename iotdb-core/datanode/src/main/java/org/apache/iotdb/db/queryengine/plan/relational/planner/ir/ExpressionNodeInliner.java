package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import java.util.Map;

public class ExpressionNodeInliner extends ExpressionRewriter<Void> {
  public static Expression replaceExpression(
      Expression expression, Map<? extends Expression, ? extends Expression> mappings) {
    return ExpressionTreeRewriter.rewriteWith(new ExpressionNodeInliner(mappings), expression);
  }

  private final Map<? extends Expression, ? extends Expression> mappings;

  public ExpressionNodeInliner(Map<? extends Expression, ? extends Expression> mappings) {
    this.mappings = mappings;
  }

  @Override
  protected Expression rewriteExpression(
      Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
    return mappings.get(node);
  }
}
