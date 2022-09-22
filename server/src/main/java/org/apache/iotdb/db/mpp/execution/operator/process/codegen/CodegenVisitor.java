/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.process.codegen;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.BetweenExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.BinaryExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ConstantExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.FunctionExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.IdentityExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.IsNullExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.LeafExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.UnaryExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.AssignmentStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.UDTFAssignmentStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.UpdateRowStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy.AccessStrategyType.MAPPABLE_ROW_BY_ROW;

public class CodegenVisitor extends ExpressionVisitor<Boolean, CodegenContext> {

  private TimestampOperand globalTimestampOperand;

  public CodegenVisitor() {}

  @Override
  public Boolean visitExpression(Expression expression, CodegenContext codegenContext) {
    // don't support TEXT type now
    if (codegenContext.inferType(expression) == TSDataType.TEXT) {
      return false;
    }
    if (codegenContext.isExpressionExisted(expression)) {
      return true;
    }
    if (codegenContext.isExpressionInput(expression)) {
      String argName = codegenContext.uniqueVarName("input");
      LeafExpressionNode leafExpressionNode = new LeafExpressionNode(argName);
      codegenContext.addExpression(expression, leafExpressionNode);
      codegenContext.addInputVarNameMap(expression.getExpressionString(), argName);
      codegenContext.addIntermediateVariable(
          createDeclareStatement(
              codegenContext.inferType(expression), new IdentityExpressionNode(argName)));
      return true;
    }
    return process(expression, codegenContext);
  }

  @Override
  public Boolean visitLogicNotExpression(
      LogicNotExpression logicNotExpression, CodegenContext codegenContext) {
    if (visitExpression(logicNotExpression.getExpression(), codegenContext)) {
      ExpressionNode subNode = codegenContext.getExpressionNode(logicNotExpression.getExpression());
      UnaryExpressionNode notNode =
          new UnaryExpressionNode(codegenContext.uniqueVarName(), subNode, "!");
      codegenContext.addExpression(logicNotExpression, notNode);

      DeclareStatement boolDeclareStatement =
          new DeclareStatement("boolean", subNode.getNodeName());
      codegenContext.addIntermediateVariable(boolDeclareStatement);
      codegenContext.addAssignmentStatement(new AssignmentStatement(subNode));
      return true;
    }
    return false;
  }

  private DeclareStatement createDeclareStatement(
      TSDataType tsDataType, ExpressionNode expressionNode) {
    DeclareStatement statement;
    switch (tsDataType) {
      case INT32:
        statement = new DeclareStatement("int", expressionNode.getNodeName());
        break;
      case INT64:
        statement = new DeclareStatement("long", expressionNode.getNodeName());
        break;
      case FLOAT:
        statement = new DeclareStatement("float", expressionNode.getNodeName());
        break;
      case DOUBLE:
        statement = new DeclareStatement("double", expressionNode.getNodeName());
        break;
      case BOOLEAN:
        statement = new DeclareStatement("boolean", expressionNode.getNodeName());
        break;
        //      case TEXT:
        //        statement = new DeclareStatement("String",expressionNode.getNodeName(),
        // expressionNode);
        //        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported for expression codegen.", tsDataType));
    }
    return statement;
  }

  @Override
  public Boolean visitNegationExpression(
      NegationExpression negationExpression, CodegenContext codegenContext) {
    if (visitExpression(negationExpression.getExpression(), codegenContext)) {

      ExpressionNode subNode = codegenContext.getExpressionNode(negationExpression.getExpression());
      UnaryExpressionNode negationNode =
          new UnaryExpressionNode(codegenContext.uniqueVarName(), subNode, "-");

      TSDataType tsDataType = codegenContext.inferType(negationExpression);
      codegenContext.addExpression(negationExpression, negationNode);

      DeclareStatement statement;
      switch (tsDataType) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
          statement = createDeclareStatement(tsDataType, negationNode);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(
                  "Data type %s is not supported for negationExpression codegen.", tsDataType));
      }

      codegenContext.addIntermediateVariable(statement);
      codegenContext.addAssignmentStatement(new AssignmentStatement(negationNode));
      return true;
    }
    return false;
  }

  @Override
  public Boolean visitBinaryExpression(
      BinaryExpression binaryExpression, CodegenContext codegenContext) {
    if (!visitExpression(binaryExpression.getRightExpression(), codegenContext)) {
      return false;
    }
    if (!visitExpression(binaryExpression.getLeftExpression(), codegenContext)) {
      return false;
    }

    ExpressionNode left = codegenContext.getExpressionNode(binaryExpression.getLeftExpression());
    String op = binaryExpression.getOperator();
    ExpressionNode right = codegenContext.getExpressionNode(binaryExpression.getRightExpression());

    BinaryExpressionNode binaryExpressionNode =
        new BinaryExpressionNode(codegenContext.uniqueVarName(), op, left, right);
    codegenContext.addExpression(binaryExpression, binaryExpressionNode);

    DeclareStatement declareStatement =
        createDeclareStatement(codegenContext.inferType(binaryExpression), binaryExpressionNode);
    codegenContext.addIntermediateVariable(declareStatement);
    codegenContext.addAssignmentStatement(new AssignmentStatement(binaryExpressionNode));
    return true;
  }

  public Boolean visitIsNullExpression(
      IsNullExpression isNullExpression, CodegenContext codegenContext) {
    Expression subExpression = isNullExpression.getExpression();
    if (!visitExpression(subExpression, codegenContext)) {
      return false;
    }
    ExpressionNode subExpressionNode = codegenContext.getExpressionNode(subExpression);

    IsNullExpressionNode isNullExpressionNode =
        new IsNullExpressionNode(
            codegenContext.uniqueVarName(), subExpressionNode, isNullExpression.isNot());

    codegenContext.addExpression(isNullExpression, isNullExpressionNode);
    codegenContext.addIntermediateVariable(
        new DeclareStatement("boolean", isNullExpressionNode.getNodeName()));
    codegenContext.addAssignmentStatement(new AssignmentStatement(isNullExpressionNode));
    return true;
  }

  @Override
  public Boolean visitBetweenExpression(
      BetweenExpression betweenExpression, CodegenContext codegenContext) {
    if (!visitExpression(betweenExpression.getFirstExpression(), codegenContext)) {
      return false;
    }
    if (!visitExpression(betweenExpression.getSecondExpression(), codegenContext)) {
      return false;
    }
    if (!visitExpression(betweenExpression.getThirdExpression(), codegenContext)) {
      return false;
    }

    boolean isNotBetween = betweenExpression.isNotBetween();

    ExpressionNode subExpressionNodeImpl =
        codegenContext.getExpressionNode(betweenExpression.getFirstExpression());
    ExpressionNode lowerNode =
        codegenContext.getExpressionNode(betweenExpression.getSecondExpression());
    ExpressionNode higherNode =
        codegenContext.getExpressionNode(betweenExpression.getThirdExpression());

    BetweenExpressionNode betweenExpressionNode =
        new BetweenExpressionNode(
            codegenContext.uniqueVarName(),
            subExpressionNodeImpl,
            lowerNode,
            higherNode,
            isNotBetween);

    codegenContext.addExpression(betweenExpression, betweenExpressionNode);

    DeclareStatement declareStatement =
        createDeclareStatement(codegenContext.inferType(betweenExpression), betweenExpressionNode);
    codegenContext.addIntermediateVariable(declareStatement);
    codegenContext.addAssignmentStatement(new AssignmentStatement(betweenExpressionNode));
    return true;
  }

  @Override
  public Boolean visitConstantOperand(
      ConstantOperand constantOperand, CodegenContext codegenContext) {
    if (!codegenContext.isExpressionExisted(constantOperand)) {
      String valueString = constantOperand.getValueString();
      codegenContext.addExpression(constantOperand, new ConstantExpressionNode(valueString));
    }
    return true;
  }

  @Override
  // since timeseries always as input, this method should never be called
  public Boolean visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, CodegenContext codegenContext) {
    return true;
  }

  @Override
  public Boolean visitTimeStampOperand(
      TimestampOperand timestampOperand, CodegenContext codegenContext) {
    // To avoid repeat of TimestampOperand
    // all TimestampOperand will be replaced with globalTimestampOperand
    if (!codegenContext.isExpressionExisted(globalTimestampOperand)) {
      if (Objects.isNull(globalTimestampOperand)) {
        globalTimestampOperand = timestampOperand;
      }
      LeafExpressionNode timestamp = new LeafExpressionNode("timestamp");
      codegenContext.addExpression(globalTimestampOperand, timestamp);
    }
    return true;
  }

  public Boolean visitInExpression(InExpression inExpression, CodegenContext codegenContext) {
    //    if (!expressionVisitor(inExpression.getExpression())) {
    //      return false;
    //    }
    //    ExpressionNode subExpressionNode =
    //        codegenContext.getExpressionNode(inExpression.getExpression());
    //    String setName = codegenContext.uniqueVarName();
    //    NewSetStatement newSetStatement =
    //        new NewSetStatement(
    //            setName,
    //            new ArrayList<>(inExpression.getValues()),
    //            inExpression.getExpression().inferTypes(typeProvider));
    //    codegenContext.addCode(newSetStatement);
    //    InExpressionNode inExpressionNode =
    //        new InExpressionNode(
    //            codegenContext.uniqueVarName(), subExpressionNode, setName,
    // inExpression.isNotIn());
    //    BoolDeclareStatement boolDeclareStatement = new BoolDeclareStatement(inExpressionNode);
    //    codegenContext.addExpression(inExpression, inExpressionNode, TSDataType.BOOLEAN);
    //    codegenContext.addCode(boolDeclareStatement);
    return false;
  }

  @Override
  public Boolean visitFunctionExpression(
      FunctionExpression functionExpression, CodegenContext codegenContext) {
    UDTFExecutor executor = codegenContext.getExecutorByFunctionExpression(functionExpression);
    if (executor.getConfigurations().getAccessStrategy().getAccessStrategyType()
        != MAPPABLE_ROW_BY_ROW) {
      return false;
    }

    List<TSDataType> inputDatatype = new ArrayList<>();
    for (Expression expression : functionExpression.getExpressions()) {
      inputDatatype.add(codegenContext.inferType(expression));
      if (!visitExpression(expression, codegenContext)) {
        return false;
      }
    }

    // get UDTFExecutor of udtf
    int udtfIndex = codegenContext.getUdtfIndex();
    String executorName = "executors[" + udtfIndex + "]";
    codegenContext.addUdtfExecutor(executor);

    // generate a simpleRow of udtf
    CodegenSimpleRow inputRow = new CodegenSimpleRow(inputDatatype.toArray(new TSDataType[0]));
    String rowName = "rows[" + udtfIndex + "]";
    codegenContext.addUdtfInput(inputRow);

    FunctionExpressionNode functionExpressionNode =
        new FunctionExpressionNode(
            codegenContext.uniqueVarName(),
            executorName,
            rowName,
            codegenContext.inferType(functionExpression));

    UpdateRowStatement updateRowStatement = new UpdateRowStatement(rowName);
    for (Expression expression : functionExpression.getExpressions()) {
      ExpressionNode subNode = codegenContext.getExpressionNode(expression);
      updateRowStatement.addData(subNode);
      functionExpressionNode.addSubExpressionNode(subNode);
    }

    // udtf may contain TimestampOperand
    // to avoid repeat of TimestampOperand, all TimestampOperand will be replaced with
    // globalTimestampOperand
    if (Objects.isNull(globalTimestampOperand)) {
      globalTimestampOperand = new TimestampOperand();
    }

    if (!codegenContext.isExpressionExisted(globalTimestampOperand)) {
      LeafExpressionNode timestamp = new LeafExpressionNode("timestamp");
      codegenContext.addExpression(globalTimestampOperand, timestamp);
    }

    codegenContext.addExpression(functionExpression, functionExpressionNode);

    DeclareStatement declareStatement =
        createDeclareStatement(
            codegenContext.inferType(functionExpression), functionExpressionNode);
    codegenContext.addIntermediateVariable(declareStatement);
    codegenContext.addAssignmentStatement(
        new UDTFAssignmentStatement(functionExpressionNode, updateRowStatement));
    return true;
  }

  @Override
  public Boolean visitUnaryExpression(
      UnaryExpression unaryExpression, CodegenContext codegenContext) {
    // like, in and some other unaryExpression haven't been handled
    return false;
  }
}
