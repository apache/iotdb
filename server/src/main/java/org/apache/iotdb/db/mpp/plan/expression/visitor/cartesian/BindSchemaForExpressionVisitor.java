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

package org.apache.iotdb.db.mpp.plan.expression.visitor.cartesian;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.TransformToExpressionVisitor;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.CompleteMeasurementSchemaVisitor;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpressions;
import static org.apache.iotdb.db.utils.TypeInferenceUtils.bindTypeForAggregationNonSeriesInputExpressions;

public class BindSchemaForExpressionVisitor extends CartesianProductVisitor<ISchemaTree> {

  @Override
  public List<Expression> visitFunctionExpression(
      FunctionExpression functionExpression, ISchemaTree schemaTree) {
    // One by one, remove the wildcards from the input expressions. In most cases, an expression
    // will produce multiple expressions after removing the wildcards. We use extendedExpressions
    // to collect the produced expressions.
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression originExpression : functionExpression.getExpressions()) {
      List<Expression> actualExpressions = process(originExpression, schemaTree);
      if (actualExpressions.isEmpty()) {
        // Let's ignore the eval of the function which has at least one non-existence series as
        // input. See IOTDB-1212: https://github.com/apache/iotdb/pull/3101
        return Collections.emptyList();
      }
      extendedExpressions.add(actualExpressions);

      // We just process first input Expression of AggregationFunction,
      // keep other input Expressions as origin and bind Type
      // If AggregationFunction need more than one input series,
      // we need to reconsider the process of it
      if (functionExpression.isBuiltInAggregationFunctionExpression()) {
        List<Expression> children = functionExpression.getExpressions();
        bindTypeForAggregationNonSeriesInputExpressions(
            functionExpression.getFunctionName(), children, extendedExpressions);
        break;
      }
    }

    // Calculate the Cartesian product of extendedExpressions to get the actual expressions after
    // removing all wildcards. We use actualExpressions to collect them.
    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());

    return reconstructFunctionExpressions(functionExpression, childExpressionsList);
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
        viewPaths.add(measurementPath);
      } else {
        nonViewActualPaths.add(measurementPath);
      }
    }
    List<Expression> reconstructTimeSeriesOperands =
        ExpressionUtils.reconstructTimeSeriesOperands(nonViewActualPaths);
    // handle logical views
    for (MeasurementPath measurementPath : viewPaths) {
      Expression replacedExpression = transformViewPath(measurementPath, schemaTree);
      replacedExpression.setViewPath(measurementPath);
      reconstructTimeSeriesOperands.add(replacedExpression);
    }
    return reconstructTimeSeriesOperands;
  }

  @Override
  public List<Expression> visitTimeStampOperand(
      TimestampOperand timestampOperand, ISchemaTree schemaTree) {
    return Collections.singletonList(timestampOperand);
  }

  @Override
  public List<Expression> visitConstantOperand(
      ConstantOperand constantOperand, ISchemaTree schemaTree) {
    return Collections.singletonList(constantOperand);
  }

  public static Expression transformViewPath(
      MeasurementPath measurementPath, ISchemaTree schemaTree) {
    IMeasurementSchema measurementSchema = measurementPath.getMeasurementSchema();
    if (measurementSchema.isLogicalView()) {
      ViewExpression viewExpression = ((LogicalViewSchema) measurementSchema).getExpression();
      // complete measurementPaths in expressions.
      Expression expression = new TransformToExpressionVisitor().process(viewExpression, null);
      expression = new CompleteMeasurementSchemaVisitor().process(expression, schemaTree);
      return expression;
    } else {
      throw new RuntimeException(
          new UnsupportedOperationException(
              "Can not construct expression using non view path in transformViewPath!"));
    }
  }
}
