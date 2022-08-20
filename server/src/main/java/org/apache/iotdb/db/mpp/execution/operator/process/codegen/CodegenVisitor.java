package org.apache.iotdb.db.mpp.execution.operator.process.codegen;

import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;

import java.util.List;

public interface CodegenVisitor {

  //  boolean unaryExpressionVisitor(UnaryExpression unaryExpression);

  boolean logicNotExpressionVisitor(LogicNotExpression logicNotExpression);

  boolean negationExpressionVisitor(NegationExpression negationExpression);

  boolean binaryExpressionVisitor(BinaryExpression binaryExpression);

  //  boolean ternaryExpressionVisitor(TernaryExpression ternaryExpression);

  boolean betweenExpressionVisitor(BetweenExpression betweenExpression);

  //  boolean leafOperandVisitor(LeafOperand leafOperand) ;

  boolean constantOperandVisitor(ConstantOperand constantOperand);

  boolean timeSeriesOperandVisitor(TimeSeriesOperand timeSeriesOperand);

  boolean timestampOperandVisitor(TimestampOperand timestampOperand);

  //  boolean mappableFunctionExpressionVisitor(FunctionExpression expression);

  boolean functionExpressionVisitor(FunctionExpression functionExpression);

  void init();

  List<String> getParameterNames();

  boolean isContainUDTFExpression();
}
