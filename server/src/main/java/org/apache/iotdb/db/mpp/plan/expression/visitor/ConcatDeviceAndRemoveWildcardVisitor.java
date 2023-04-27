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
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperands;
import static org.apache.iotdb.db.utils.TypeInferenceUtils.bindTypeForAggregationNonSeriesInputExpressions;

public class ConcatDeviceAndRemoveWildcardVisitor
    extends CartesianProductVisitor<ConcatDeviceAndRemoveWildcardVisitor.Context> {
  @Override
  public List<Expression> visitFunctionExpression(
      FunctionExpression functionExpression, Context context) {
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression suffixExpression : functionExpression.getExpressions()) {
      List<Expression> concatedExpression = process(suffixExpression, context);
      if (concatedExpression != null && !concatedExpression.isEmpty()) {
        extendedExpressions.add(concatedExpression);
      }

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

    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
    return reconstructFunctionExpressions(functionExpression, childExpressionsList);
  }

  @Override
  public List<Expression> visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, Context context) {
    PartialPath measurement = timeSeriesOperand.getPath();
    PartialPath concatPath = context.getDevicePath().concatPath(measurement);

    List<MeasurementPath> actualPaths =
        context.getSchemaTree().searchMeasurementPaths(concatPath).left;
    if (actualPaths.isEmpty()) {
      return Collections.emptyList();
    }
    List<PartialPath> noStarPaths = new ArrayList<>(actualPaths);
    return reconstructTimeSeriesOperands(noStarPaths);
  }

  @Override
  public List<Expression> visitTimeStampOperand(
      TimestampOperand timestampOperand, Context context) {
    return Collections.singletonList(timestampOperand);
  }

  @Override
  public List<Expression> visitConstantOperand(ConstantOperand constantOperand, Context context) {
    return Collections.singletonList(constantOperand);
  }

  public static class Context {
    private final PartialPath devicePath;
    private final ISchemaTree schemaTree;

    public Context(PartialPath devicePath, ISchemaTree schemaTree) {
      this.devicePath = devicePath;
      this.schemaTree = schemaTree;
    }

    public PartialPath getDevicePath() {
      return devicePath;
    }

    public ISchemaTree getSchemaTree() {
      return schemaTree;
    }
  }
}
