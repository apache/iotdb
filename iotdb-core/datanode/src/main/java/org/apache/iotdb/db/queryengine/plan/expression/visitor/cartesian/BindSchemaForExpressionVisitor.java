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

package org.apache.iotdb.db.queryengine.plan.expression.visitor.cartesian;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.CompleteMeasurementSchemaVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.view.visitor.TransformToExpressionVisitor;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructFunctionExpressionsWithMemoryCheck;
import static org.apache.iotdb.db.utils.TypeInferenceUtils.bindTypeForBuiltinAggregationNonSeriesInputExpressions;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME;

public class BindSchemaForExpressionVisitor
    extends CartesianProductVisitor<BindSchemaForExpressionVisitor.Context> {

  @Override
  public List<Expression> visitFunctionExpression(
      FunctionExpression functionExpression, Context context) {

    if (COUNT_TIME.equalsIgnoreCase(functionExpression.getFunctionName())) {
      List<Expression> usedExpressions =
          functionExpression.getExpressions().stream()
              .flatMap(e -> process(e, context).stream())
              .collect(Collectors.toList());

      Expression countTimeExpression =
          new FunctionExpression(
              COUNT_TIME,
              new LinkedHashMap<>(),
              Collections.singletonList(new TimestampOperand()),
              usedExpressions);
      return Collections.singletonList(countTimeExpression);
    }

    // One by one, remove the wildcards from the input expressions. In most cases, an expression
    // will produce multiple expressions after removing the wildcards. We use extendedExpressions
    // to collect the produced expressions.
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression originExpression : functionExpression.getExpressions()) {
      List<Expression> actualExpressions = process(originExpression, context);
      if (actualExpressions.isEmpty()) {
        // Let's ignore the eval of the function which has at least one non-existence series as
        // input. See IOTDB-1212: https://github.com/apache/iotdb/pull/3101
        return Collections.emptyList();
      }
      extendedExpressions.add(actualExpressions);

      // We just process first input Expression of Count_IF,
      // keep other input Expressions as origin and bind Type
      if (SqlConstant.COUNT_IF.equalsIgnoreCase(functionExpression.getFunctionName())) {
        List<Expression> children = functionExpression.getExpressions();
        bindTypeForBuiltinAggregationNonSeriesInputExpressions(
            functionExpression.getFunctionName(), children, extendedExpressions);
        break;
      }
    }

    // Calculate the Cartesian product of extendedExpressions to get the actual expressions after
    // removing all wildcards. We use actualExpressions to collect them.
    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());

    return reconstructFunctionExpressionsWithMemoryCheck(
        functionExpression, childExpressionsList, context.getQueryContext());
  }

  @Override
  public List<Expression> visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, Context context) {
    PartialPath timeSeriesOperandPath = timeSeriesOperand.getPath();
    List<MeasurementPath> actualPaths =
        context.getSchemaTree().searchMeasurementPaths(timeSeriesOperandPath).left;
    // process logical view
    List<MeasurementPath> nonViewActualPaths = new ArrayList<>();
    List<MeasurementPath> viewPaths = new ArrayList<>();
    for (MeasurementPath measurementPath : actualPaths) {
      if (measurementPath.getMeasurementSchema().isLogicalView()) {
        viewPaths.add(measurementPath);
      } else {
        nonViewActualPaths.add(measurementPath);
      }
    }
    List<Expression> reconstructTimeSeriesOperands =
        ExpressionUtils.reconstructTimeSeriesOperandsWithMemoryCheck(
            timeSeriesOperand, nonViewActualPaths, context.getQueryContext());
    // handle logical views
    for (MeasurementPath measurementPath : viewPaths) {
      Expression replacedExpression = transformViewPath(measurementPath, context.getSchemaTree());
      replacedExpression.setViewPath(measurementPath);
      reconstructTimeSeriesOperands.add(replacedExpression);
    }
    return reconstructTimeSeriesOperands;
  }

  @Override
  public List<Expression> visitTimeStampOperand(
      TimestampOperand timestampOperand, Context context) {
    return Collections.singletonList(timestampOperand);
  }

  @Override
  public List<Expression> visitConstantOperand(ConstantOperand constantOperand, Context context) {
    return Collections.singletonList(constantOperand);
  }

  public static Expression transformViewPath(
      MeasurementPath measurementPath, ISchemaTree schemaTree) {
    IMeasurementSchema measurementSchema = measurementPath.getMeasurementSchema();
    if (!measurementSchema.isLogicalView()) {
      throw new IllegalArgumentException(
          "Can not construct expression using non view path in transformViewPath!");
    }

    ViewExpression viewExpression = ((LogicalViewSchema) measurementSchema).getExpression();
    // complete measurementPaths in expressions.
    Expression expression = new TransformToExpressionVisitor().process(viewExpression, null);
    expression = new CompleteMeasurementSchemaVisitor().process(expression, schemaTree);
    return expression;
  }

  public static class Context implements QueryContextProvider {
    private final ISchemaTree schemaTree;
    private final MPPQueryContext queryContext;

    public Context(final ISchemaTree schemaTree, final MPPQueryContext queryContext) {
      this.schemaTree = schemaTree;
      Validate.notNull(queryContext, "QueryContext is null");
      this.queryContext = queryContext;
    }

    public ISchemaTree getSchemaTree() {
      return schemaTree;
    }

    @Override
    public MPPQueryContext getQueryContext() {
      return queryContext;
    }
  }
}
