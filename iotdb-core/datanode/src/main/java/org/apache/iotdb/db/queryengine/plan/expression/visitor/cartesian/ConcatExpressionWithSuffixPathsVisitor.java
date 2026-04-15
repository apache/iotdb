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

package org.apache.iotdb.db.queryengine.plan.expression.visitor.cartesian;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.external.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructFunctionExpressionsWithMemoryCheck;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperandsWithMemoryCheck;

public class ConcatExpressionWithSuffixPathsVisitor
    extends CartesianProductVisitor<ConcatExpressionWithSuffixPathsVisitor.Context> {
  @Override
  public List<Expression> visitFunctionExpression(
      FunctionExpression functionExpression, Context context) {
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    if (SqlConstant.COUNT_IF.equalsIgnoreCase(functionExpression.getFunctionName())) {
      // We just process first input Expression of Count_IF,
      // keep other input Expressions as origin
      extendedExpressions.add(process(functionExpression.getExpressions().get(0), context));
      List<Expression> children = functionExpression.getExpressions();
      for (int i = 1; i < children.size(); i++) {
        extendedExpressions.add(Collections.singletonList(children.get(i)));
      }
    } else {
      functionExpression
          .getExpressions()
          .forEach(expression -> extendedExpressions.add(process(expression, context)));
    }

    List<List<Expression>> childExpressionsList = new ArrayList<>();
    cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
    return reconstructFunctionExpressionsWithMemoryCheck(
        functionExpression, childExpressionsList, context.getQueryContext());
  }

  @Override
  public List<Expression> visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, Context context) {
    PartialPath rawPath = timeSeriesOperand.getPath();
    List<PartialPath> actualPaths = new ArrayList<>();
    if (rawPath.getFullPath().startsWith(SqlConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
      actualPaths.add(rawPath);
      context.getPatternTree().appendPathPattern(rawPath);
    } else {
      for (PartialPath prefixPath : context.getPrefixPaths()) {
        PartialPath concatPath = prefixPath.concatPath(rawPath);
        context.getPatternTree().appendPathPattern(concatPath);
        actualPaths.add(concatPath);
      }
    }
    return reconstructTimeSeriesOperandsWithMemoryCheck(
        timeSeriesOperand, actualPaths, context.getQueryContext());
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

  public static class Context implements QueryContextProvider {
    private final List<PartialPath> prefixPaths;
    private final PathPatternTree patternTree;

    private final MPPQueryContext queryContext;

    public Context(
        final List<PartialPath> prefixPaths,
        final PathPatternTree patternTree,
        final MPPQueryContext queryContext) {
      this.prefixPaths = prefixPaths;
      this.patternTree = patternTree;
      Validate.notNull(queryContext, "QueryContext is null");
      this.queryContext = queryContext;
    }

    public List<PartialPath> getPrefixPaths() {
      return prefixPaths;
    }

    public PathPatternTree getPatternTree() {
      return patternTree;
    }

    @Override
    public MPPQueryContext getQueryContext() {
      return queryContext;
    }
  }
}
