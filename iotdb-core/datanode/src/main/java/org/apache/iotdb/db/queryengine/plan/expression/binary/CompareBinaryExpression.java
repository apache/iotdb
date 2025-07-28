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

package org.apache.iotdb.db.queryengine.plan.expression.binary;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;

import java.nio.ByteBuffer;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionType.GREATER_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionType.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionType.LESS_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionType.LESS_THAN;

public abstract class CompareBinaryExpression extends BinaryExpression {

  protected CompareBinaryExpression(Expression leftExpression, Expression rightExpression) {
    super(leftExpression, rightExpression);
  }

  protected CompareBinaryExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public static ExpressionType flipType(ExpressionType origin) {
    switch (origin) {
      case EQUAL_TO:
      case NON_EQUAL:
        return origin;
      case GREATER_THAN:
        return LESS_THAN;
      case GREATER_EQUAL:
        return LESS_EQUAL;
      case LESS_THAN:
        return GREATER_THAN;
      case LESS_EQUAL:
        return GREATER_EQUAL;
      default:
        throw new IllegalArgumentException("Unsupported expression type: " + origin);
    }
  }

  @Override
  public boolean isCompareBinaryExpression() {
    return true;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitCompareBinaryExpression(this, context);
  }
}
