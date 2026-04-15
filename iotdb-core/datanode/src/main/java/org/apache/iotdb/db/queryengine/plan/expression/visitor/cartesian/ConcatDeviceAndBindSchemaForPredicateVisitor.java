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
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;

import org.apache.tsfile.external.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructFunctionExpressionsWithMemoryCheck;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperandsWithMemoryCheck;
import static org.apache.iotdb.db.queryengine.plan.expression.visitor.cartesian.BindSchemaForExpressionVisitor.transformViewPath;

public class ConcatDeviceAndBindSchemaForPredicateVisitor
    extends CartesianProductVisitor<ConcatDeviceAndBindSchemaForPredicateVisitor.Context> {

  @Override
  public List<Expression> visitFunctionExpression(FunctionExpression predicate, Context context) {
    if (predicate.isAggregationFunctionExpression() && context.isWhere()) {
      throw new SemanticException("aggregate functions are not supported in WHERE clause");
    }
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression suffixExpression : predicate.getExpressions()) {
      extendedExpressions.add(process(suffixExpression, context));
    }
    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
    return reconstructFunctionExpressionsWithMemoryCheck(
        predicate, childExpressionsList, context.getQueryContext());
  }

  @Override
  public List<Expression> visitTimeSeriesOperand(TimeSeriesOperand predicate, Context context) {
    PartialPath measurement = predicate.getPath();
    PartialPath concatPath = context.getDevicePath().concatPath(measurement);

    List<MeasurementPath> nonViewPathList = new ArrayList<>();
    List<MeasurementPath> viewPathList = new ArrayList<>();
    List<MeasurementPath> actualPaths =
        context.getSchemaTree().searchMeasurementPaths(concatPath).left;
    if (actualPaths.isEmpty()) {
      return Collections.singletonList(new NullOperand());
    }
    for (MeasurementPath measurementPath : actualPaths) {
      if (measurementPath.getMeasurementSchema().isLogicalView()) {
        viewPathList.add(measurementPath);
      } else {
        nonViewPathList.add(measurementPath);
      }
    }

    List<Expression> reconstructTimeSeriesOperands =
        reconstructTimeSeriesOperandsWithMemoryCheck(
            predicate, nonViewPathList, context.getQueryContext());
    for (MeasurementPath measurementPath : viewPathList) {
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
    private final boolean isWhere;

    private final MPPQueryContext queryContext;

    public Context(
        final PartialPath devicePath,
        final ISchemaTree schemaTree,
        final boolean isWhere,
        final MPPQueryContext queryContext) {
      this.devicePath = devicePath;
      this.schemaTree = schemaTree;
      this.isWhere = isWhere;
      Validate.notNull(queryContext, "QueryContext is null");
      this.queryContext = queryContext;
    }

    public PartialPath getDevicePath() {
      return devicePath;
    }

    public ISchemaTree getSchemaTree() {
      return schemaTree;
    }

    public boolean isWhere() {
      return isWhere;
    }

    @Override
    public MPPQueryContext getQueryContext() {
      return queryContext;
    }
  }
}
