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
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionTypeAnalyzer;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.List;

public class GetMeasurementExpressionVisitor extends ReconstructVisitor<Analysis> {

  @Override
  public Expression process(Expression expression, Analysis analysis) {
    if (expression.getViewPath() != null) {
      PartialPath viewPath = expression.getViewPath();
      return new TimeSeriesOperand(
          new MeasurementPath(
              new PartialPath(viewPath.getMeasurement(), false),
              ExpressionTypeAnalyzer.analyzeExpression(analysis, expression)));
    }
    return expression.accept(this, analysis);
  }

  @Override
  public Expression visitFunctionExpression(
      FunctionExpression functionExpression, Analysis analysis) {
    List<Expression> childExpressions = new ArrayList<>();
    for (Expression suffixExpression : functionExpression.getExpressions()) {
      childExpressions.add(process(suffixExpression, analysis));
    }
    return new FunctionExpression(
        functionExpression.getFunctionName(),
        functionExpression.getFunctionAttributes(),
        childExpressions,
        functionExpression.getCountTimeExpressions());
  }

  @Override
  public Expression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Analysis analysis) {
    MeasurementPath rawPath = (MeasurementPath) timeSeriesOperand.getPath();
    String measurementName =
        rawPath.isMeasurementAliasExists()
            ? rawPath.getMeasurementAlias()
            : rawPath.getMeasurement();
    MeasurementPath measurementPath =
        new MeasurementPath(
            new PartialPath(measurementName, false), rawPath.getMeasurementSchema());
    measurementPath.setTagMap(rawPath.getTagMap());
    return new TimeSeriesOperand(measurementPath);
  }
}
