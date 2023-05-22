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
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.TransformToExpressionVisitor;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class RemoveWildcardAndViewInExpressionVisitor extends RemoveWildcardInExpressionVisitor {
  private TransformToExpressionVisitor transformToExpressionVisitor = null;

  private CompleteMeasurementSchemaVisitor completeMeasurementSchemaVisitor = null;

  boolean hasProcessedLogicalView = false;

  public RemoveWildcardAndViewInExpressionVisitor() {
    super();
    this.transformToExpressionVisitor = new TransformToExpressionVisitor();
    this.completeMeasurementSchemaVisitor = new CompleteMeasurementSchemaVisitor();
    this.hasProcessedLogicalView = false;
  }

  public boolean isHasProcessedLogicalView() {
    return this.hasProcessedLogicalView;
  }

  private Expression transformViewPath(MeasurementPath measurementPath, ISchemaTree schemaTree) {
    IMeasurementSchema measurementSchema = measurementPath.getMeasurementSchema();
    if (measurementSchema.isLogicalView()) {
      ViewExpression viewExpression = ((LogicalViewSchema) measurementSchema).getExpression();
      // complete measurementPaths in expressions.
      Expression expression = this.transformToExpressionVisitor.process(viewExpression, null);
      expression = this.completeMeasurementSchemaVisitor.process(expression, schemaTree);
      return expression;
    } else {
      throw new RuntimeException(
          new UnsupportedOperationException(
              "Can not construct expression using non view path in transformViewPath!"));
    }
  }

  @Override
  public List<Expression> visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, ISchemaTree schemaTree) {
    PartialPath timeSeriesOperandPath = timeSeriesOperand.getPath();
    List<MeasurementPath> actualPaths =
        schemaTree.searchMeasurementPaths(timeSeriesOperandPath).left;
    // process logical view
    List<MeasurementPath> nonViewActualPaths = new ArrayList<>();
    List<MeasurementPath> viewPaths = new ArrayList<>();
    for (MeasurementPath measurementPath : actualPaths) {
      if (measurementPath.getMeasurementSchema().isLogicalView()) {
        this.hasProcessedLogicalView = true;
        viewPaths.add(measurementPath);
      } else {
        nonViewActualPaths.add(measurementPath);
      }
    }
    List<Expression> reconstructTimeSeriesOperands =
        ExpressionUtils.reconstructTimeSeriesOperands(nonViewActualPaths);
    // handle logical views
    for (MeasurementPath measurementPath : viewPaths) {
      Expression replacedExpression = this.transformViewPath(measurementPath, schemaTree);
      replacedExpression.setStringWithLogicalView(measurementPath.getFullPath());
      reconstructTimeSeriesOperands.add(replacedExpression);
    }
    return reconstructTimeSeriesOperands;
  }
}
