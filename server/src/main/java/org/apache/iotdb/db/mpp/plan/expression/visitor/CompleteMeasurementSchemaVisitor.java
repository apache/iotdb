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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.view.BrokenViewException;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

import java.util.ArrayList;
import java.util.List;

public class CompleteMeasurementSchemaVisitor extends ExpressionVisitor<Expression, ISchemaTree> {

  @Override
  public Expression process(Expression expression, ISchemaTree schemaTree) {
    return expression.accept(this, schemaTree);
  }

  @Override
  public Expression visitExpression(Expression expression, ISchemaTree schemaTree) {
    return expression;
  }

  @Override
  public Expression visitUnaryExpression(UnaryExpression unaryExpression, ISchemaTree schemaTree) {
    unaryExpression.setExpression(this.process(unaryExpression.getExpression(), schemaTree));
    return unaryExpression;
  }

  @Override
  public Expression visitBinaryExpression(
      BinaryExpression binaryExpression, ISchemaTree schemaTree) {
    binaryExpression.setLeftExpression(
        this.process(binaryExpression.getLeftExpression(), schemaTree));
    binaryExpression.setRightExpression(
        this.process(binaryExpression.getRightExpression(), schemaTree));
    return binaryExpression;
  }

  @Override
  public Expression visitTernaryExpression(
      TernaryExpression ternaryExpression, ISchemaTree schemaTree) {
    ternaryExpression.setFirstExpression(
        this.process(ternaryExpression.getFirstExpression(), schemaTree));
    ternaryExpression.setSecondExpression(
        this.process(ternaryExpression.getSecondExpression(), schemaTree));
    ternaryExpression.setThirdExpression(
        this.process(ternaryExpression.getThirdExpression(), schemaTree));
    return ternaryExpression;
  }

  @Override
  public Expression visitFunctionExpression(
      FunctionExpression functionExpression, ISchemaTree schemaTree) {
    List<Expression> children = functionExpression.getExpressions();
    List<Expression> replacedChildren = new ArrayList<>();
    for (Expression child : children) {
      replacedChildren.add(this.process(child, schemaTree));
    }
    functionExpression.setExpressions(replacedChildren);
    return functionExpression;
  }

  @Override
  public Expression visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, ISchemaTree schemaTree) {
    PartialPath path = timeSeriesOperand.getPath();
    try {
      try {
        path.getMeasurementSchema();
      } catch (Exception notAMeasurementPath) {
        List<MeasurementPath> actualPaths = schemaTree.searchMeasurementPaths(path).left;
        if (actualPaths.size() != 1) {
          throw new BrokenViewException(path.getFullPath(), actualPaths);
        }
        return new TimeSeriesOperand(actualPaths.get(0));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return timeSeriesOperand;
  }
}
