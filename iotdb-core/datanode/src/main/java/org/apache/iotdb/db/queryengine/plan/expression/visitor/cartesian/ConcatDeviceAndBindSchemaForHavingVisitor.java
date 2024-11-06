/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;

import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.expression.visitor.cartesian.BindSchemaForExpressionVisitor.transformViewPath;

public class ConcatDeviceAndBindSchemaForHavingVisitor
    extends ConcatDeviceAndBindSchemaForExpressionVisitor {
  @Override
  public List<Expression> visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, Context context) {
    PartialPath measurement = timeSeriesOperand.getPath();
    PartialPath concatPath = context.getDevicePath().concatPath(measurement);

    List<MeasurementPath> actualPaths =
        context.getSchemaTree().searchMeasurementPaths(concatPath).left;
    if (actualPaths.isEmpty()) {
      return Collections.singletonList(
          new TimeSeriesOperand(new MeasurementPath(concatPath, TSDataType.UNKNOWN)));
    }

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
}
