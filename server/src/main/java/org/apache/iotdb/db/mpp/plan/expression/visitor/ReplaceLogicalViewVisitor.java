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

package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.GetSourcePathsVisitor;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.TransformToExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

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

  public ReplaceLogicalViewVisitor() {
    this.transformToExpressionVisitor = new TransformToExpressionVisitor();
    this.getSourcePathsVisitor = new GetSourcePathsVisitor();
  }

  private Expression transform(ViewExpression viewExpression) {
    return this.transformToExpressionVisitor.process(viewExpression, null);
  }

  private List<PartialPath> collectSourcePaths(ViewExpression viewExpression) {
    return this.getSourcePathsVisitor.process(viewExpression, null);
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
    return functionExpression;
  }

  @Override
  public Expression visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, List<PartialPath> context) {
    PartialPath path = timeSeriesOperand.getPath();
    try {
      IMeasurementSchema measurementSchema = path.getMeasurementSchema();
      if (measurementSchema.isLogicalView()) {
        ViewExpression viewExpression = ((LogicalViewSchema) measurementSchema).getExpression();
        Expression result = this.transform(viewExpression);
        // record paths in this viewExpression
        context.addAll(this.collectSourcePaths(viewExpression));
        return result;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return timeSeriesOperand;
  }
}
