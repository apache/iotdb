package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.Collections;
import java.util.List;

public class CollectAggregationExpressionsVisitor extends CollectVisitor {
  @Override
  public List<Expression> visitFunctionExpression(
      FunctionExpression functionExpression, Void context) {
    if (functionExpression.isBuiltInAggregationFunctionExpression())
      return Collections.singletonList(functionExpression);
    return collectFromChild(functionExpression);
  }

  @Override
  public List<Expression> visitLeafOperand(LeafOperand leafOperand, Void context) {
    return Collections.emptyList();
  }
}
