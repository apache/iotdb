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
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.TransformToExpressionVisitor;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperands;

public class RemoveWildcardAndViewInFilterVisitor extends RemoveWildcardInFilterVisitor {

  private final TransformToExpressionVisitor transformToExpressionVisitor;

  private final CompleteMeasurementSchemaVisitor completeMeasurementSchemaVisitor;

  boolean hasProcessedLogicalView;

  public RemoveWildcardAndViewInFilterVisitor() {
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
    List<Expression> reconstructTimeSeriesOperands = reconstructTimeSeriesOperands(nonViewPathList);
    for (MeasurementPath measurementPath : viewPathList) {
      Expression replacedExpression =
          this.transformViewPath(measurementPath, context.getSchemaTree());
      replacedExpression.setViewPathOfThisExpression(measurementPath);
      reconstructTimeSeriesOperands.add(replacedExpression);
    }
    return reconstructTimeSeriesOperands;
  }
}
