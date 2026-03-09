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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.external.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructBinaryExpressionsWithMemoryCheck;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructFunctionExpressionsWithMemoryCheck;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperandsWithMemoryCheck;
import static org.apache.iotdb.db.queryengine.plan.expression.visitor.cartesian.BindSchemaForExpressionVisitor.transformViewPath;
import static org.apache.iotdb.db.utils.TypeInferenceUtils.bindTypeForBuiltinAggregationNonSeriesInputExpressions;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME;

public class BindSchemaForPredicateVisitor
    extends CartesianProductVisitor<BindSchemaForPredicateVisitor.Context> {

  @Override
  public List<Expression> visitBinaryExpression(
      BinaryExpression binaryExpression, Context context) {
    List<Expression> leftExpressions =
        process(binaryExpression.getLeftExpression(), context.notRootClone());
    List<Expression> rightExpressions =
        process(binaryExpression.getRightExpression(), context.notRootClone());
    if (context.isRoot() && binaryExpression.getExpressionType() == ExpressionType.LOGIC_AND) {
      List<Expression> resultExpressions = new ArrayList<>(leftExpressions);
      resultExpressions.addAll(rightExpressions);
      return resultExpressions;
    }
    return reconstructBinaryExpressionsWithMemoryCheck(
        binaryExpression, leftExpressions, rightExpressions, context.getQueryContext());
  }

  @Override
  public List<Expression> visitFunctionExpression(FunctionExpression predicate, Context context) {
    if (COUNT_TIME.equalsIgnoreCase(predicate.getFunctionName())) {
      List<Expression> usedExpressions =
          predicate.getExpressions().stream()
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

    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression suffixExpression : predicate.getExpressions()) {
      extendedExpressions.add(
          process(
              suffixExpression,
              new Context(
                  context.getPrefixPaths(),
                  context.getSchemaTree(),
                  false,
                  context.getQueryContext())));

      // We just process first input Expression of Count_IF,
      // keep other input Expressions as origin and bind Type
      if (SqlConstant.COUNT_IF.equalsIgnoreCase(predicate.getFunctionName())) {
        List<Expression> children = predicate.getExpressions();
        bindTypeForBuiltinAggregationNonSeriesInputExpressions(
            predicate.getFunctionName(), children, extendedExpressions);
        break;
      }
    }
    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
    return reconstructFunctionExpressionsWithMemoryCheck(
        predicate, childExpressionsList, context.getQueryContext());
  }

  @Override
  public List<Expression> visitTimeSeriesOperand(TimeSeriesOperand predicate, Context context) {
    PartialPath filterPath = predicate.getPath();
    List<PartialPath> concatPaths = new ArrayList<>();
    if (!filterPath.getFirstNode().equals(SqlConstant.ROOT)) {
      context.getPrefixPaths().forEach(prefix -> concatPaths.add(prefix.concatPath(filterPath)));
    } else {
      // do nothing in the case of "where root.d1.s1 > 5"
      concatPaths.add(filterPath);
    }

    List<MeasurementPath> nonViewPathList = new ArrayList<>();
    List<MeasurementPath> viewPathList = new ArrayList<>();
    for (PartialPath concatPath : concatPaths) {
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
    }
    List<Expression> reconstructTimeSeriesOperands =
        reconstructTimeSeriesOperandsWithMemoryCheck(
            predicate, nonViewPathList, context.getQueryContext());
    for (MeasurementPath measurementPath : viewPathList) {
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

  public static class Context implements QueryContextProvider {
    private final List<PartialPath> prefixPaths;
    private final ISchemaTree schemaTree;
    private final boolean isRoot;

    private final MPPQueryContext queryContext;

    public Context(
        final List<PartialPath> prefixPaths,
        final ISchemaTree schemaTree,
        final boolean isRoot,
        final MPPQueryContext queryContext) {
      this.prefixPaths = prefixPaths;
      this.schemaTree = schemaTree;
      this.isRoot = isRoot;
      Validate.notNull(queryContext, "QueryContext is null");
      this.queryContext = queryContext;
    }

    public Context notRootClone() {
      return new Context(this.prefixPaths, this.schemaTree, false, queryContext);
    }

    public List<PartialPath> getPrefixPaths() {
      return prefixPaths;
    }

    public ISchemaTree getSchemaTree() {
      return schemaTree;
    }

    public boolean isRoot() {
      return isRoot;
    }

    @Override
    public MPPQueryContext getQueryContext() {
      return queryContext;
    }
  }
}
