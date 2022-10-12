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

package org.apache.iotdb.db.mpp.plan.expression.unary;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class IsNullExpression extends UnaryExpression {
  private final boolean isNot;

  public IsNullExpression(Expression expression, boolean isNot) {
    super(expression);
    this.isNot = isNot;
  }

  public IsNullExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
    isNot = ReadWriteIOUtils.readBool(byteBuffer);
  }

  public boolean isNot() {
    return isNot;
  }

  @Override
  protected String getExpressionStringInternal() {
    return expression + " IS " + (isNot ? "NOT " : "") + "NULL";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.IS_NULL;
  }

  @Override
  protected Expression constructExpression(Expression childExpression) {
    return new IsNullExpression(childExpression, isNot);
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isNot, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(isNot, stream);
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitIsNullExpression(this, context);
  }
}
