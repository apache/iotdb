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

package org.apache.iotdb.db.queryengine.plan.expression.expression.expression.unary;

import org.apache.iotdb.db.node_commons.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.expression.expression.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.expression.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.expression.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.expression.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.expression.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.expression.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.expression.expression.visitor.ExpressionVisitor;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.nio.ByteBuffer;

public class NegationExpression extends UnaryExpression {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NegationExpression.class);

  public NegationExpression(Expression expression) {
    super(expression);
  }

  public NegationExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
  }

  @Override
  public String getExpressionStringInternal() {
    return expression instanceof TimeSeriesOperand
            || expression instanceof FunctionExpression
            || expression instanceof NullOperand
            || (expression instanceof ConstantOperand
                && !((ConstantOperand) expression).isNegativeNumber())
        ? "-" + expression.getExpressionString()
        : "-(" + expression.getExpressionString() + ")";
  }

  @Override
  public String getOutputSymbolInternal() {
    return expression instanceof TimeSeriesOperand
            || expression instanceof FunctionExpression
            || expression instanceof NullOperand
            || (expression instanceof ConstantOperand
                && !((ConstantOperand) expression).isNegativeNumber())
        ? "-" + expression.getOutputSymbol()
        : "-(" + expression.getOutputSymbol() + ")";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.NEGATION;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitNegationExpression(this, context);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(expression);
  }
}
