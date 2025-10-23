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

package org.apache.iotdb.db.queryengine.plan.expression.visitor;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.schemaengine.schemaregion.view.visitor.GetSourcePathsVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.view.visitor.TransformToExpressionVisitor;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * step 1. check whether this expression contains logical view, that means check TimeSeriesOperand
 * which has the LogicalViewSchema.
 *
 * <p>step 2. replace that TimeSeriesOperand with expression recorded in LogicalViewSchema (view
 * expression).
 *
 * <p>step 3. record paths that appeared in view expression. They should be fetched later.
 */
public class ReplaceLogicalViewVisitor extends ExpressionVisitor<Expression, List<PartialPath>> {

  private TransformToExpressionVisitor transformToExpressionVisitor = null;

  private GetSourcePathsVisitor getSourcePathsVisitor = null;

  private boolean hasProcessedAggregationFunction = false;

  /** The paths that are new added, which should be re-fetched. */
  private List<PartialPath> newAddedPathList = null;

  public ReplaceLogicalViewVisitor() {
    this.transformToExpressionVisitor = new TransformToExpressionVisitor();
    this.getSourcePathsVisitor = new GetSourcePathsVisitor();
    this.resetHadProcessedAggregationFunction();
    this.newAddedPathList = new ArrayList<>();
  }

  /**
   * This function will check the expression you put in, find the TimeSeriesOperand which has
   * LogicalViewSchema. These TimeSeriesOperand will be replaced with logical view expression, and
   * this function will record paths that appeared in view expression. The logical view you replaced
   * have INCOMPLETE path information, and use PartialPath in TimeSeriesOperand. This may cause
   * ERROR, therefore you should call completeMeasurementPathCausedByView() later, make sure the
   * path info is complete and using MeasurementPath with full MeasurementSchema.
   *
   * @param expression the expression you want to check.
   * @return pair of replaced expression and whether replacement was happened. 'True' means the
   *     expression you put in contains logical view, and has been replaced. 'False' means there is
   *     no need to modify the expression you put in.
   */
  public Pair<Expression, Boolean> replaceViewInThisExpression(Expression expression) {
    // step 1. check whether this expression contains logical view, that means finding
    // TimeSeriesOperand which has
    // the LogicalViewSchema.
    // step 2. replace that TimeSeriesOperand with expression recorded in LogicalViewSchema (view
    // expression).
    // step 3. record paths that appeared in view expression. They should be fetched, then you can
    // use fetched schema
    // to complete new added TimeSeriesOperand.
    int oldSize = this.newAddedPathList.size();
    Expression result = this.process(expression, this.newAddedPathList);
    int newSize = this.newAddedPathList.size();
    if (oldSize != newSize) {
      return new Pair<>(result, true);
    }
    return new Pair<>(expression, false);
  }

  public List<PartialPath> getNewAddedPathList() {
    return this.newAddedPathList;
  }

  private Expression transform(ViewExpression viewExpression) {
    return this.transformToExpressionVisitor.process(viewExpression, null);
  }

  private List<PartialPath> collectSourcePaths(ViewExpression viewExpression) {
    return this.getSourcePathsVisitor.process(viewExpression, null);
  }

  public boolean getHadProcessedAggregationFunction() {
    return this.hasProcessedAggregationFunction;
  }

  public void resetHadProcessedAggregationFunction() {
    this.hasProcessedAggregationFunction = false;
  }

  @Override
  public Expression process(Expression expression, List<PartialPath> context) {
    return expression.accept(this, context);
  }

  @Override
  public Expression visitExpression(Expression expression, List<PartialPath> context) {
    return expression;
  }

  @Override
  public Expression visitUnaryExpression(
      UnaryExpression unaryExpression, List<PartialPath> context) {
    unaryExpression.setExpression(this.process(unaryExpression.getExpression(), context));
    return unaryExpression;
  }

  @Override
  public Expression visitBinaryExpression(
      BinaryExpression binaryExpression, List<PartialPath> context) {
    binaryExpression.setLeftExpression(this.process(binaryExpression.getLeftExpression(), context));
    binaryExpression.setRightExpression(
        this.process(binaryExpression.getRightExpression(), context));
    return binaryExpression;
  }

  @Override
  public Expression visitTernaryExpression(
      TernaryExpression ternaryExpression, List<PartialPath> context) {
    ternaryExpression.setFirstExpression(
        this.process(ternaryExpression.getFirstExpression(), context));
    ternaryExpression.setSecondExpression(
        this.process(ternaryExpression.getSecondExpression(), context));
    ternaryExpression.setThirdExpression(
        this.process(ternaryExpression.getThirdExpression(), context));
    return ternaryExpression;
  }

  @Override
  public Expression visitFunctionExpression(
      FunctionExpression functionExpression, List<PartialPath> context) {
    List<Expression> children = functionExpression.getExpressions();
    List<Expression> replacedChildren = new ArrayList<>();
    for (Expression child : children) {
      replacedChildren.add(this.process(child, context));
    }
    functionExpression.setExpressions(replacedChildren);
    if (functionExpression.isAggregationFunctionExpression()) {
      this.hasProcessedAggregationFunction = true;
    }
    return functionExpression;
  }

  @Override
  public Expression visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, List<PartialPath> context) {
    PartialPath path = timeSeriesOperand.getPath();
    try {
      if (path instanceof MeasurementPath) {
        IMeasurementSchema measurementSchema = path.getMeasurementSchema();
        if (measurementSchema.isLogicalView()) {
          ViewExpression viewExpression = ((LogicalViewSchema) measurementSchema).getExpression();
          Expression result = this.transform(viewExpression);
          // record paths in this viewExpression
          context.addAll(this.collectSourcePaths(viewExpression));
          return result;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return timeSeriesOperand;
  }
}
