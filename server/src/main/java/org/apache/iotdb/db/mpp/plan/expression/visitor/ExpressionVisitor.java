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

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

/**
 * This class provides a visitor of {@link Expression}, which can be extended to create a visitor
 * which only needs to handle a subset of the available methods.
 *
 * @param <R> The return type of the visit operation.
 */
public abstract class ExpressionVisitor<R> {

  public R process(Expression expression) {
    return expression.accept(this);
  }

  public abstract R visitExpression(Expression expression);

  public R visitUnaryExpression(UnaryExpression unaryExpression) {
    return visitExpression(unaryExpression);
  }

  public R visitBinaryExpression(BinaryExpression binaryExpression) {
    return visitExpression(binaryExpression);
  }

  public R visitTernaryExpression(TernaryExpression ternaryExpression) {
    return visitExpression(ternaryExpression);
  }

  public R visitFunctionExpression(FunctionExpression functionExpression) {
    return visitExpression(functionExpression);
  }

  public R visitTimeStampOperand(TimestampOperand timestampOperand) {
    return visitExpression(timestampOperand);
  }

  public R visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand) {
    return visitExpression(timeSeriesOperand);
  }

  public R visitConstantOperand(ConstantOperand constantOperand) {
    return visitExpression(constantOperand);
  }
}
