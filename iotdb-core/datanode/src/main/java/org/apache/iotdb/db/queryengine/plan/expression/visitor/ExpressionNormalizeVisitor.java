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
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.List;

public class ExpressionNormalizeVisitor extends ReconstructVisitor<Void> {

  @Override
  public Expression process(Expression expression, Void context) {
    Expression resultExpression = expression.accept(this, context);
    resultExpression.setViewPath(null);
    return resultExpression;
  }

  @Override
  public Expression visitFunctionExpression(FunctionExpression functionExpression, Void context) {
    List<Expression> childResult = new ArrayList<>();
    functionExpression.getExpressions().forEach(child -> childResult.add(process(child, null)));
    return new FunctionExpression(
        functionExpression.getFunctionName().toLowerCase(),
        functionExpression.getFunctionAttributes(),
        childResult,
        functionExpression.getCountTimeExpressions());
  }

  @Override
  public Expression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
    PartialPath rawPath = timeSeriesOperand.getPath();
    PartialPath newPath = rawPath.copy();
    if (newPath.isMeasurementAliasExists()) {
      ((MeasurementPath) newPath).removeMeasurementAlias();
    }
    return new TimeSeriesOperand(newPath);
  }
}
