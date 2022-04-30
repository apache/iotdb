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

package org.apache.iotdb.db.query.expression.unary;

import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ExpressionType;
import org.apache.iotdb.db.query.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;
import org.apache.iotdb.db.query.udf.core.transformer.unary.ArithmeticNegationTransformer;

import java.nio.ByteBuffer;

public class NegationExpression extends UnaryExpression {

  public NegationExpression(Expression expression) {
    super(expression);
  }

  public NegationExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
  }

  @Override
  protected Transformer constructTransformer(LayerPointReader pointReader) {
    return new ArithmeticNegationTransformer(pointReader);
  }

  @Override
  protected Expression constructExpression(Expression childExpression) {
    return new NegationExpression(childExpression);
  }

  @Override
  public String getExpressionStringInternal() {
    return expression instanceof TimeSeriesOperand
            || expression instanceof FunctionExpression
            || (expression instanceof ConstantOperand
                && !((ConstantOperand) expression).isNegativeNumber())
        ? "-" + expression
        : "-(" + expression + ")";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.NEGATION;
  }
}
