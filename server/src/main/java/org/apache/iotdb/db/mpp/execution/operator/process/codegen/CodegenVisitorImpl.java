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
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.IsNullExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.LeafExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.UnaryExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.Statement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.UpdateRowStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.BoolDeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.DoubleDeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.FloatDeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.IntDeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.LongDeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.StringDeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy.AccessStrategyType.MAPPABLE_ROW_BY_ROW;

public class CodegenVisitorImpl implements CodegenVisitor {

  // collect variables showed in expression
  private final HashSet<String> argsName;

  private TimestampOperand globalTimestampOperand;

  private boolean containUDTFExpression;

  private final CodegenContext codegenContext;

  private final UDTFContext udtfContext;

  private final TypeProvider typeProvider;

  public CodegenVisitorImpl(
      TypeProvider typeProvider, UDTFContext udtfContext, CodegenContext codegenContext) {
    argsName = new HashSet<>();
    containUDTFExpression = false;
    this.typeProvider = typeProvider;
    this.udtfContext = udtfContext;
    this.codegenContext = codegenContext;
  }

  @Override
  public void init() {
    argsName.clear();
    containUDTFExpression = false;
  }

  @Override
  public List<String> getParameterNames() {
    return Arrays.asList(argsName.toArray(new String[0]));
  }

  @Override
  public boolean logicNotExpressionVisitor(LogicNotExpression logicNotExpression) {
    if (!codegenContext.isExpressionExisted(logicNotExpression)) {
      if (logicNotExpression.getExpression().codegenAccept(this)) {
        ExpressionNode subNode =
            codegenContext.getExpressionNode(logicNotExpression.getExpression());
        UnaryExpressionNode notNode =
            new UnaryExpressionNode(codegenContext.uniqueVarName(), subNode, "!");
        codegenContext.addExpression(
            logicNotExpression, notNode, logicNotExpression.inferTypes(typeProvider));

        BoolDeclareStatement boolDeclareStatement = new BoolDeclareStatement(subNode);
        codegenContext.addCode(boolDeclareStatement);
        return true;
      }
      return false;
    }
    return true;
  }

  private Statement createDeclareStatement(TSDataType tsDataType, ExpressionNode expressionNode) {
    Statement statement;
    switch (tsDataType) {
      case INT32:
        statement = new IntDeclareStatement(expressionNode);
        break;
      case INT64:
        statement = new LongDeclareStatement(expressionNode);
        break;
      case FLOAT:
        statement = new FloatDeclareStatement(expressionNode);
        break;
      case DOUBLE:
        statement = new DoubleDeclareStatement(expressionNode);
        break;
      case BOOLEAN:
        statement = new BoolDeclareStatement(expressionNode);
        break;
      case TEXT:
        statement = new StringDeclareStatement(expressionNode);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Data type %s is not supported for negationExpression codegen.", tsDataType));
    }
    return statement;
  }

  @Override
  public boolean negationExpressionVisitor(NegationExpression negationExpression) {
    if (!codegenContext.isExpressionExisted(negationExpression)) {
      if (negationExpression.getExpression().codegenAccept(this)) {

        ExpressionNode subNode =
            codegenContext.getExpressionNode(negationExpression.getExpression());
        UnaryExpressionNode negationNode =
            new UnaryExpressionNode(codegenContext.uniqueVarName(), subNode, "-");

        TSDataType tsDataType = negationExpression.inferTypes(typeProvider);
        codegenContext.addExpression(negationExpression, negationNode, tsDataType);

        Statement statement;
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

        codegenContext.addCode(statement);
        return true;
      }
      return false;
    }
    return true;
  }

  @Override
  public boolean binaryExpressionVisitor(BinaryExpression binaryExpression) {
    if (!codegenContext.isExpressionExisted(binaryExpression)) {
      if (!binaryExpression.getRightExpression().codegenAccept(this)) {
        return false;
      }
      if (!binaryExpression.getLeftExpression().codegenAccept(this)) {
        return false;
      }

      ExpressionNode left = codegenContext.getExpressionNode(binaryExpression.getLeftExpression());
      String op = binaryExpression.getOperator();
      ExpressionNode right =
          codegenContext.getExpressionNode(binaryExpression.getRightExpression());

      BinaryExpressionNode binaryExpressionNode =
          new BinaryExpressionNode(codegenContext.uniqueVarName(), op, left, right);
      codegenContext.addExpression(
          binaryExpression, binaryExpressionNode, binaryExpression.inferTypes(typeProvider));

      Statement declareStatement =
          createDeclareStatement(binaryExpression.inferTypes(typeProvider), binaryExpressionNode);
      codegenContext.addCode(declareStatement);
    }

    return true;
  }

  public boolean isNullExpressionVisitor(IsNullExpression isNullExpression) {
    if (!codegenContext.isExpressionExisted(isNullExpression)) {
      Expression subExpression = isNullExpression.getExpression();
      if (!subExpression.codegenAccept(this)) {
        return false;
      }
      ExpressionNode subExpressionNode = codegenContext.getExpressionNode(subExpression);

      IsNullExpressionNode isNullExpressionNode =
          new IsNullExpressionNode(
              codegenContext.uniqueVarName(), subExpressionNode, isNullExpression.isNot());

      codegenContext.addExpression(isNullExpression, isNullExpressionNode, TSDataType.BOOLEAN);
      codegenContext.addCode(new BoolDeclareStatement(isNullExpressionNode));
    }
    return true;
  }

  @Override
  public boolean betweenExpressionVisitor(BetweenExpression betweenExpression) {
    if (!codegenContext.isExpressionExisted(betweenExpression)) {
      if (!betweenExpression.getFirstExpression().codegenAccept(this)) {
        return false;
      }
      if (!betweenExpression.getSecondExpression().codegenAccept(this)) {
        return false;
      }
      if (!betweenExpression.getThirdExpression().codegenAccept(this)) {
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

      codegenContext.addExpression(
          betweenExpression, betweenExpressionNode, betweenExpression.inferTypes(typeProvider));

      Statement declareStatement =
          createDeclareStatement(betweenExpression.inferTypes(typeProvider), betweenExpressionNode);
      codegenContext.addCode(declareStatement);
      return true;
    }
    return true;
  }

  @Override
  public boolean constantOperandVisitor(ConstantOperand constantOperand) {
    if (!codegenContext.isExpressionExisted(constantOperand)) {
      String valueString = constantOperand.getValueString();
      codegenContext.addExpression(
          constantOperand,
          new ConstantExpressionNode(valueString),
          constantOperand.inferTypes(typeProvider));
      codegenContext.addConstExpr(constantOperand);
    }
    return true;
  }

  @Override
  public boolean timeSeriesOperandVisitor(TimeSeriesOperand timeSeriesOperand) {
    if (!codegenContext.isExpressionExisted(timeSeriesOperand)) {
      String fullPath = timeSeriesOperand.getPath().getFullPath();
      String argName = fullPath.replace('.', '_');

      LeafExpressionNode leafExpressionNode = new LeafExpressionNode(argName);
      codegenContext.addExpression(
          timeSeriesOperand, leafExpressionNode, timeSeriesOperand.inferTypes(typeProvider));
      codegenContext.addInputExpr(timeSeriesOperand);
    }

    return true;
  }

  @Override
  public boolean timestampOperandVisitor(TimestampOperand timestampOperand) {
    // To avoid repeat of TimestampOperand
    // all TimestampOperand will be replaced with globalTimestampOperand
    if (!codegenContext.isExpressionExisted(globalTimestampOperand)) {
      if (Objects.isNull(globalTimestampOperand)) {
        globalTimestampOperand = timestampOperand;
      }
      LeafExpressionNode timestamp = new LeafExpressionNode("timestamp");
      codegenContext.addExpression(globalTimestampOperand, timestamp, TSDataType.INT64);
      codegenContext.addInputExpr(globalTimestampOperand);
    }
    return true;
  }

  @Override
  public boolean isContainUDTFExpression() {
    return containUDTFExpression;
  }

  @Override
  public boolean functionExpressionVisitor(FunctionExpression functionExpression) {
    if (!codegenContext.isExpressionExisted(functionExpression)) {
      UDTFExecutor executor = udtfContext.getExecutorByFunctionExpression(functionExpression);
      if (executor.getConfigurations().getAccessStrategy().getAccessStrategyType()
          != MAPPABLE_ROW_BY_ROW) {
        return false;
      }

      // get UDTFExecutor of udtf
      String executorName = codegenContext.uniqueVarName("executor");
      codegenContext.addUdtfExecutor(executorName, executor);

      List<TSDataType> inputDatatype = new ArrayList<>();
      for (Expression expression : functionExpression.getExpressions()) {
        inputDatatype.add(expression.inferTypes(typeProvider));
        if (!expression.codegenAccept(this)) {
          return false;
        }
      }

      // generate a simpleRow of udtf
      CodegenSimpleRow inputRow = new CodegenSimpleRow(inputDatatype.toArray(new TSDataType[0]));
      String rowName = codegenContext.uniqueVarName("row");
      codegenContext.addUdtfInput(rowName, inputRow);

      FunctionExpressionNode functionExpressionNode =
          new FunctionExpressionNode(
              codegenContext.uniqueVarName(),
              executorName,
              rowName,
              functionExpression.inferTypes(typeProvider));

      UpdateRowStatement updateRowStatement = new UpdateRowStatement(rowName);
      for (Expression expression : functionExpression.getExpressions()) {
        ExpressionNode subNode = codegenContext.getExpressionNode(expression);
        updateRowStatement.addData(subNode);
        functionExpressionNode.addSubExpressionNode(subNode);
      }

      codegenContext.addCode(updateRowStatement);

      // udtf may contain TimestampOperand
      // to avoid repeat of TimestampOperand, all TimestampOperand will be replaced with
      // globalTimestampOperand
      if (Objects.isNull(globalTimestampOperand)) {
        globalTimestampOperand = new TimestampOperand();
      }

      if (!codegenContext.isExpressionExisted(globalTimestampOperand)) {
        LeafExpressionNode timestamp = new LeafExpressionNode("timestamp");
        codegenContext.addExpression(globalTimestampOperand, timestamp, TSDataType.INT64);
        codegenContext.addInputExpr(globalTimestampOperand);
      }

      containUDTFExpression = true;
      codegenContext.addExpression(
          functionExpression, functionExpressionNode, functionExpression.inferTypes(typeProvider));
      Statement declareStatement =
          createDeclareStatement(
              functionExpression.inferTypes(typeProvider), functionExpressionNode);
      codegenContext.addCode(declareStatement);
    }
    return true;
  }
}
