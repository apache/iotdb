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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFInformationInferrer;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExpressionTypeAnalyzer {

  private final Map<NodeRef<Expression>, TSDataType> expressionTypes = new LinkedHashMap<>();

  private ExpressionTypeAnalyzer() {}

  public static TSDataType analyzeExpression(Analysis analysis, Expression expression) {
    if (!analysis.getExpressionTypes().containsKey(NodeRef.of(expression))) {
      ExpressionTypeAnalyzer analyzer = new ExpressionTypeAnalyzer();
      analyzer.analyze(expression);

      addExpressionTypes(analysis, analyzer);
    }
    return analysis.getType(expression);
  }

  public static void analyzeExpression(
      Map<NodeRef<Expression>, TSDataType> types, Expression expression) {
    ExpressionTypeAnalyzer analyzer = new ExpressionTypeAnalyzer();
    analyzer.analyze(expression);

    types.putAll(analyzer.getExpressionTypes());
  }

  private static void addExpressionTypes(Analysis analysis, ExpressionTypeAnalyzer analyzer) {
    analysis.addTypes(analyzer.getExpressionTypes());
  }

  public TSDataType analyze(Expression expression) {
    Visitor visitor = new Visitor();
    return visitor.process(expression, null);
  }

  public Map<NodeRef<Expression>, TSDataType> getExpressionTypes() {
    return expressionTypes;
  }

  private class Visitor extends ExpressionVisitor<TSDataType, Void> {

    @Override
    public TSDataType process(Expression expression, Void context) {
      // don't double process a expression
      TSDataType dataType = expressionTypes.get(NodeRef.of(expression));
      if (dataType != null) {
        return dataType;
      }
      return super.process(expression, context);
    }

    @Override
    public TSDataType visitExpression(Expression expression, Void context) {
      throw new UnsupportedOperationException(
          "Unsupported expression type: " + expression.getClass().getName());
    }

    @Override
    public TSDataType visitInExpression(InExpression inExpression, Void context) {
      process(inExpression.getExpression(), null);
      return setExpressionType(inExpression, TSDataType.BOOLEAN);
    }

    @Override
    public TSDataType visitIsNullExpression(IsNullExpression isNullExpression, Void context) {
      process(isNullExpression.getExpression(), null);
      return setExpressionType(isNullExpression, TSDataType.BOOLEAN);
    }

    @Override
    public TSDataType visitLikeExpression(LikeExpression likeExpression, Void context) {
      checkInputExpressionDataType(
          likeExpression.getExpression().getExpressionString(),
          process(likeExpression.getExpression(), null),
          TSDataType.TEXT);
      return setExpressionType(likeExpression, TSDataType.BOOLEAN);
    }

    @Override
    public TSDataType visitRegularExpression(RegularExpression regularExpression, Void context) {
      checkInputExpressionDataType(
          regularExpression.getExpression().getExpressionString(),
          process(regularExpression.getExpression(), null),
          TSDataType.TEXT);
      return setExpressionType(regularExpression, TSDataType.BOOLEAN);
    }

    @Override
    public TSDataType visitLogicNotExpression(LogicNotExpression logicNotExpression, Void context) {
      checkInputExpressionDataType(
          logicNotExpression.getExpression().getExpressionString(),
          process(logicNotExpression.getExpression(), null),
          TSDataType.BOOLEAN);
      return setExpressionType(logicNotExpression, TSDataType.BOOLEAN);
    }

    @Override
    public TSDataType visitNegationExpression(NegationExpression negationExpression, Void context) {
      TSDataType inputExpressionType = process(negationExpression.getExpression(), null);
      checkInputExpressionDataType(
          negationExpression.getExpression().getExpressionString(),
          inputExpressionType,
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE);
      return setExpressionType(negationExpression, inputExpressionType);
    }

    @Override
    public TSDataType visitArithmeticBinaryExpression(
        ArithmeticBinaryExpression arithmeticBinaryExpression, Void context) {
      checkInputExpressionDataType(
          arithmeticBinaryExpression.getLeftExpression().getExpressionString(),
          process(arithmeticBinaryExpression.getLeftExpression(), null),
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE);
      checkInputExpressionDataType(
          arithmeticBinaryExpression.getRightExpression().getExpressionString(),
          process(arithmeticBinaryExpression.getRightExpression(), null),
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE);
      return setExpressionType(arithmeticBinaryExpression, TSDataType.DOUBLE);
    }

    @Override
    public TSDataType visitLogicBinaryExpression(
        LogicBinaryExpression logicBinaryExpression, Void context) {
      checkInputExpressionDataType(
          logicBinaryExpression.getLeftExpression().getExpressionString(),
          process(logicBinaryExpression.getLeftExpression(), null),
          TSDataType.BOOLEAN);
      checkInputExpressionDataType(
          logicBinaryExpression.getRightExpression().getExpressionString(),
          process(logicBinaryExpression.getRightExpression(), null),
          TSDataType.BOOLEAN);
      return setExpressionType(logicBinaryExpression, TSDataType.BOOLEAN);
    }

    @Override
    public TSDataType visitCompareBinaryExpression(
        CompareBinaryExpression compareBinaryExpression, Void context) {
      final TSDataType leftExpressionDataType =
          process(compareBinaryExpression.getLeftExpression(), null);
      final TSDataType rightExpressionDataType =
          process(compareBinaryExpression.getRightExpression(), null);

      if (leftExpressionDataType != null
          && rightExpressionDataType != null
          && !leftExpressionDataType.equals(rightExpressionDataType)) {
        final String leftExpressionString =
            compareBinaryExpression.getLeftExpression().getExpressionString();
        final String rightExpressionString =
            compareBinaryExpression.getRightExpression().getExpressionString();

        if (TSDataType.BOOLEAN.equals(leftExpressionDataType)
            || TSDataType.BOOLEAN.equals(rightExpressionDataType)) {
          checkInputExpressionDataType(
              leftExpressionString, leftExpressionDataType, TSDataType.BOOLEAN);
          checkInputExpressionDataType(
              rightExpressionString, rightExpressionDataType, TSDataType.BOOLEAN);
        } else if (TSDataType.TEXT.equals(leftExpressionDataType)
            || TSDataType.TEXT.equals(rightExpressionDataType)) {
          checkInputExpressionDataType(
              leftExpressionString, leftExpressionDataType, TSDataType.TEXT);
          checkInputExpressionDataType(
              rightExpressionString, rightExpressionDataType, TSDataType.TEXT);
        } else {
          checkInputExpressionDataType(
              leftExpressionString,
              leftExpressionDataType,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE);
          checkInputExpressionDataType(
              rightExpressionString,
              rightExpressionDataType,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE);
        }
      }

      return setExpressionType(compareBinaryExpression, TSDataType.BOOLEAN);
    }

    @Override
    public TSDataType visitBetweenExpression(BetweenExpression betweenExpression, Void context) {
      process(betweenExpression.getFirstExpression(), null);
      process(betweenExpression.getSecondExpression(), null);
      process(betweenExpression.getThirdExpression(), null);
      return setExpressionType(betweenExpression, TSDataType.BOOLEAN);
    }

    @Override
    public TSDataType visitFunctionExpression(FunctionExpression functionExpression, Void context) {
      List<Expression> inputExpressions = functionExpression.getExpressions();
      for (Expression expression : inputExpressions) {
        process(expression, null);
      }

      if (functionExpression.isBuiltInAggregationFunctionExpression()) {
        return setExpressionType(
            functionExpression,
            TypeInferenceUtils.getAggrDataType(
                functionExpression.getFunctionName(),
                expressionTypes.get(NodeRef.of(inputExpressions.get(0)))));
      }
      if (functionExpression.isBuiltInScalarFunction()) {
        return setExpressionType(
            functionExpression,
            TypeInferenceUtils.getBuiltInScalarFunctionDataType(
                functionExpression, expressionTypes.get(NodeRef.of(inputExpressions.get(0)))));
      } else {
        return setExpressionType(
            functionExpression,
            new UDTFInformationInferrer(functionExpression.getFunctionName())
                .inferOutputType(
                    inputExpressions.stream()
                        .map(Expression::getExpressionString)
                        .collect(Collectors.toList()),
                    inputExpressions.stream()
                        .map(f -> expressionTypes.get(NodeRef.of(f)))
                        .collect(Collectors.toList()),
                    functionExpression.getFunctionAttributes()));
      }
    }

    @Override
    public TSDataType visitTimeStampOperand(TimestampOperand timestampOperand, Void context) {
      return setExpressionType(timestampOperand, TSDataType.INT64);
    }

    @Override
    public TSDataType visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
      return setExpressionType(timeSeriesOperand, timeSeriesOperand.getPath().getSeriesType());
    }

    @Override
    public TSDataType visitConstantOperand(ConstantOperand constantOperand, Void context) {
      return setExpressionType(constantOperand, constantOperand.getDataType());
    }

    @Override
    public TSDataType visitNullOperand(NullOperand nullOperand, Void context) {
      return null;
    }

    @Override
    public TSDataType visitCaseWhenThenExpression(
        CaseWhenThenExpression caseWhenThenExpression, Void context) {
      Set<TSDataType> typeSet = new HashSet<>();
      for (WhenThenExpression whenThenExpression :
          caseWhenThenExpression.getWhenThenExpressions()) {
        typeSet.add(process(whenThenExpression, context));
      }
      if (!(caseWhenThenExpression.getElseExpression() instanceof NullOperand)) {
        typeSet.add(process(caseWhenThenExpression.getElseExpression(), context));
      }
      // if TEXT exists, every branch need to be TEXT
      if (typeSet.contains(TSDataType.TEXT)) {
        if (typeSet.stream().anyMatch(tsDataType -> tsDataType != TSDataType.TEXT)) {
          throw new SemanticException(
              "CASE expression: TEXT and other types cannot exist at the same time");
        }
        return setExpressionType(caseWhenThenExpression, TSDataType.TEXT);
      }
      // if BOOLEAN exists, every branch need to be BOOLEAN
      if (typeSet.contains(TSDataType.BOOLEAN)) {
        if (typeSet.stream().anyMatch(tsDataType -> tsDataType != TSDataType.BOOLEAN)) {
          throw new SemanticException(
              "CASE expression: BOOLEAN and other types cannot exist at the same time");
        }
        return setExpressionType(caseWhenThenExpression, TSDataType.BOOLEAN);
      }
      // other 4 TSDataType can exist at the same time
      // because they can be transformed by Type, finally treated as DOUBLE
      return setExpressionType(caseWhenThenExpression, TSDataType.DOUBLE);
    }

    @Override
    public TSDataType visitWhenThenExpression(WhenThenExpression whenThenExpression, Void context) {
      TSDataType whenType = process(whenThenExpression.getWhen(), context);
      if (!whenType.equals(TSDataType.BOOLEAN)) {
        throw new SemanticException(
            String.format(
                "The expression in the WHEN clause must return BOOLEAN. expression: %s, actual data type: %s.",
                whenThenExpression.getWhen().getExpressionString(), whenType.name()));
      }
      TSDataType thenType = process(whenThenExpression.getThen(), context);
      return setExpressionType(whenThenExpression, thenType);
    }

    private TSDataType setExpressionType(Expression expression, TSDataType type) {
      expressionTypes.put(NodeRef.of(expression), type);
      return type;
    }

    private void checkInputExpressionDataType(
        String expressionString, TSDataType actual, TSDataType... expected) {
      for (TSDataType type : expected) {
        if (actual == null || actual.equals(type)) {
          return;
        }
      }
      throw new SemanticException(
          String.format(
              "Invalid input expression data type. expression: %s, actual data type: %s, expected data type(s): %s.",
              expressionString, actual.name(), Arrays.toString(expected)));
    }
  }
}
