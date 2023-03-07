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
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.List;

public class GetMeasurementExpressionVisitor extends ReconstructVisitor<Void> {
  @Override
  public Expression visitFunctionExpression(FunctionExpression functionExpression, Void context) {
    List<Expression> childExpressions = new ArrayList<>();
    for (Expression suffixExpression : functionExpression.getExpressions()) {
      childExpressions.add(process(suffixExpression, null));
    }
    return new FunctionExpression(
        ((FunctionExpression) functionExpression).getFunctionName(),
        ((FunctionExpression) functionExpression).getFunctionAttributes(),
        childExpressions);
  }

  @Override
  public Expression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
    MeasurementPath rawPath = (MeasurementPath) timeSeriesOperand.getPath();
    PartialPath measurement = new PartialPath(rawPath.getMeasurement(), false);
    MeasurementPath measurementWithSchema =
        new MeasurementPath(measurement, rawPath.getMeasurementSchema());
    if (rawPath.isMeasurementAliasExists()) {
      measurementWithSchema.setMeasurementAlias(rawPath.getMeasurementAlias());
    }
    measurementWithSchema.setTagMap(rawPath.getTagMap());
    return new TimeSeriesOperand(measurementWithSchema);
  }
}
