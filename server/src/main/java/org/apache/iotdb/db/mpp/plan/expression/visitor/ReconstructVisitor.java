package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructAllKindsOfExpression;

/**
 * Collect result from child, then reconstruct. For example, two child each give me 1 result, I
 * should use them to reconstruct 1 new result to upper level.
 */
public class ReconstructVisitor<C> extends ExpressionAnalyzeVisitor<Expression, C> {
  // process every child, then reconstruct a new expression
  public Expression reconstructFromChild(Expression expression, C context) {
    List<Expression> childResult = new ArrayList<>();
    expression.getExpressions().forEach(child -> childResult.add(process(child, context)));
    return reconstructAllKindsOfExpression(expression, childResult);
  }

  @Override
  public Expression visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    return reconstructFromChild(ternaryExpression, context);
  }

  @Override
  public Expression visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    return reconstructFromChild(binaryExpression, context);
  }

  @Override
  public Expression visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    return reconstructFromChild(unaryExpression, context);
  }

  @Override
  public Expression visitLeafOperand(LeafOperand leafOperand, C context) {
    return leafOperand;
  }
}
