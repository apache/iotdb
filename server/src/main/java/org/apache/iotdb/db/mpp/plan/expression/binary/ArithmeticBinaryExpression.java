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

package org.apache.iotdb.db.mpp.plan.expression.binary;

import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;

public abstract class ArithmeticBinaryExpression extends BinaryExpression {

  protected ArithmeticBinaryExpression(Expression leftExpression, Expression rightExpression) {
    super(leftExpression, rightExpression);
  }

  protected ArithmeticBinaryExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  @Override
  public final TSDataType inferTypes(TypeProvider typeProvider) {
    final String expressionString = toString();
    if (!typeProvider.containsTypeInfoOf(expressionString)) {
      checkInputExpressionDataType(
          leftExpression.toString(),
          leftExpression.inferTypes(typeProvider),
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE);
      checkInputExpressionDataType(
          rightExpression.toString(),
          rightExpression.inferTypes(typeProvider),
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE);
      typeProvider.setType(expressionString, TSDataType.DOUBLE);
    }
    return TSDataType.DOUBLE;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitArithmeticBinaryExpression(this, context);
  }
}
