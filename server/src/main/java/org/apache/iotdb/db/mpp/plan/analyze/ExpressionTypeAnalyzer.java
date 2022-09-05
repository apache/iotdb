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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.LinkedHashMap;
import java.util.Map;

public class ExpressionTypeAnalyzer {

  private final Map<NodeRef<Expression>, TSDataType> expressionTypes = new LinkedHashMap<>();

  private ExpressionTypeAnalyzer(Analysis analysis, TypeProvider types) {}

  public static void analyzeExpression(Analysis analysis, Expression expression) {
    ExpressionTypeAnalyzer analyzer = new ExpressionTypeAnalyzer(analysis, TypeProvider.empty());
    analyzer.analyze(expression);

    updateAnalysis(analysis, analyzer);
  }

  private static void updateAnalysis(Analysis analysis, ExpressionTypeAnalyzer analyzer) {}

  public TSDataType analyze(Expression expression) {
    Visitor visitor = new Visitor();
    return visitor.process(expression, null);
  }

  private class Visitor extends ExpressionVisitor<TSDataType, Void> {

    @Override
    public TSDataType process(Expression expression, Void context) {
      // don't double process a expression
      TSDataType dataType = expressionTypes.get(NodeRef.of(expression));
      if (dataType != null) {
        return dataType;
      }
      return super.process(expression, context);
    }

    @Override
    public TSDataType visitExpression(Expression expression, Void context) {
        throw new UnsupportedOperationException(
                "Unsupported expression type: " + expression.getClass().getName());
    }

    @Override
    public TSDataType visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
      expressionTypes.put(NodeRef.of(timeSeriesOperand), TSDataType.INT64);
      return timeSeriesOperand.getPath().getSeriesType();
    }

    @Override
    public TSDataType visitTimeStampOperand(TimestampOperand timestampOperand, Void context) {
      expressionTypes.put(NodeRef.of(timestampOperand), TSDataType.INT64);
      return TSDataType.INT64;
    }

  }
}
