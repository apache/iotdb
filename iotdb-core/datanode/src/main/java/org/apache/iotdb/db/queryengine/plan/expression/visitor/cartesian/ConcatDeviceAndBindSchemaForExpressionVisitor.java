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
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.external.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructFunctionExpressionsWithMemoryCheck;
import static org.apache.iotdb.db.queryengine.plan.expression.visitor.cartesian.BindSchemaForExpressionVisitor.transformViewPath;
import static org.apache.iotdb.db.utils.TypeInferenceUtils.bindTypeForBuiltinAggregationNonSeriesInputExpressions;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME;

public class ConcatDeviceAndBindSchemaForExpressionVisitor
    extends CartesianProductVisitor<ConcatDeviceAndBindSchemaForExpressionVisitor.Context> {

  @Override
  public List<Expression> visitFunctionExpression(
      FunctionExpression functionExpression, Context context) {
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression suffixExpression : functionExpression.getExpressions()) {
      List<Expression> concatExpression = process(suffixExpression, context);
      if (concatExpression != null && !concatExpression.isEmpty()) {
        extendedExpressions.add(concatExpression);
      }

      // We just process first input Expression of COUNT_IF,
      // keep other input Expressions as origin and bind Type
      if (SqlConstant.COUNT_IF.equalsIgnoreCase(functionExpression.getFunctionName())) {
        List<Expression> children = functionExpression.getExpressions();
        bindTypeForBuiltinAggregationNonSeriesInputExpressions(
            functionExpression.getFunctionName(), children, extendedExpressions);
        break;
      }
    }

    if (COUNT_TIME.equalsIgnoreCase(functionExpression.getFunctionName())) {
      List<Expression> usedExpressions =
          extendedExpressions.stream().flatMap(Collection::stream).collect(Collectors.toList());

      Expression countTimeExpression =
          new FunctionExpression(
              COUNT_TIME,
              new LinkedHashMap<>(),
              Collections.singletonList(new TimestampOperand()),
              usedExpressions);
      return Collections.singletonList(countTimeExpression);
    }

    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
    return reconstructFunctionExpressionsWithMemoryCheck(
        functionExpression, childExpressionsList, context.getQueryContext());
  }

  @Override
  public List<Expression> visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, Context context) {
    PartialPath measurement = timeSeriesOperand.getPath();
    PartialPath concatPath = context.getDevicePath().concatPath(measurement);

    List<MeasurementPath> actualPaths =
        context.getSchemaTree().searchMeasurementPaths(concatPath).left;
    if (actualPaths.isEmpty()) {
      return Collections.emptyList();
    }

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
      if (!(replacedExpression instanceof TimeSeriesOperand)) {
        throw new SemanticException(
            "Only writable view timeseries are supported in ALIGN BY DEVICE queries.");
      }

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

  public static class Context implements QueryContextProvider {
    private final PartialPath devicePath;
    private final ISchemaTree schemaTree;

    private final MPPQueryContext queryContext;

    public Context(
        final PartialPath devicePath,
        final ISchemaTree schemaTree,
        final MPPQueryContext queryContext) {
      this.devicePath = devicePath;
      this.schemaTree = schemaTree;
      Validate.notNull(queryContext, "QueryContext is null");
      this.queryContext = queryContext;
    }

    public PartialPath getDevicePath() {
      return devicePath;
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
