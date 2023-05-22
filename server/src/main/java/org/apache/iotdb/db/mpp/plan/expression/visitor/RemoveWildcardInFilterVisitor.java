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
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructBinaryExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperands;
import static org.apache.iotdb.db.utils.TypeInferenceUtils.bindTypeForAggregationNonSeriesInputExpressions;

public class RemoveWildcardInFilterVisitor
    extends CartesianProductVisitor<RemoveWildcardInFilterVisitor.Context> {
  @Override
  public List<Expression> visitBinaryExpression(
      BinaryExpression binaryExpression, Context context) {
    List<Expression> leftExpressions =
        process(binaryExpression.getLeftExpression(), context.notRootClone());
    List<Expression> rightExpressions =
        process(binaryExpression.getRightExpression(), context.notRootClone());
    if (context.isRoot() && binaryExpression.getExpressionType() == ExpressionType.LOGIC_AND) {
      List<Expression> resultExpressions = new ArrayList<>(leftExpressions);
      resultExpressions.addAll(rightExpressions);
      return resultExpressions;
    }
    return reconstructBinaryExpressions(
        binaryExpression.getExpressionType(), leftExpressions, rightExpressions);
  }

  @Override
  public List<Expression> visitFunctionExpression(FunctionExpression predicate, Context context) {
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression suffixExpression : predicate.getExpressions()) {
      extendedExpressions.add(
          process(
              suffixExpression,
              new Context(context.getPrefixPaths(), context.getSchemaTree(), false)));

      // We just process first input Expression of AggregationFunction,
      // keep other input Expressions as origin and bind Type
      // If AggregationFunction need more than one input series,
      // we need to reconsider the process of it
      if (predicate.isBuiltInAggregationFunctionExpression()) {
        List<Expression> children = predicate.getExpressions();
        bindTypeForAggregationNonSeriesInputExpressions(
            predicate.getFunctionName(), children, extendedExpressions);
        break;
      }
    }
    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
    return reconstructFunctionExpressions(predicate, childExpressionsList);
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

    List<PartialPath> noStarPaths = new ArrayList<>();
    for (PartialPath concatPath : concatPaths) {
      List<MeasurementPath> actualPaths =
          context.getSchemaTree().searchMeasurementPaths(concatPath).left;
      if (actualPaths.isEmpty()) {
        return Collections.singletonList(new NullOperand());
      }
      noStarPaths.addAll(actualPaths);
    }
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
    private final List<PartialPath> prefixPaths;
    private final ISchemaTree schemaTree;
    private final boolean isRoot;

    public Context(List<PartialPath> prefixPaths, ISchemaTree schemaTree, boolean isRoot) {
      this.prefixPaths = prefixPaths;
      this.schemaTree = schemaTree;
      this.isRoot = isRoot;
    }

    public Context notRootClone() {
      return new Context(this.prefixPaths, this.schemaTree, false);
    }

    public List<PartialPath> getPrefixPaths() {
      return prefixPaths;
    }

    public ISchemaTree getSchemaTree() {
      return schemaTree;
    }

    public boolean isRoot() {
      return isRoot;
    }
  }
}
