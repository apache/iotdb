package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.cartesianProductAllKindsOfExpression;

public class CartesianProductVisitor<C> extends ExpressionAnalyzeVisitor<List<Expression>, C> {
  private List<Expression> cartesianProductFromChild(Expression expression, C context) {
    List<List<Expression>> childResultsList = new ArrayList<>();
    expression.getExpressions().forEach(child -> childResultsList.add(process(child, context)));
    return cartesianProductAllKindsOfExpression(expression, childResultsList);
  }

  @Override
  public List<Expression> visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    return cartesianProductFromChild(ternaryExpression, context);
  }

  @Override
  public List<Expression> visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    return cartesianProductFromChild(binaryExpression, context);
  }

  @Override
  public List<Expression> visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    return cartesianProductFromChild(unaryExpression, context);
  }
}
