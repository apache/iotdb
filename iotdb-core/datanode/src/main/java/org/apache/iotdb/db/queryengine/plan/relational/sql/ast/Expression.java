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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.plan.relational.sql.util.ExpressionFormatter;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Expression extends Node {

  protected Expression(@Nullable NodeLocation location) {
    super(location);
  }

  /** Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead. */
  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExpression(this, context);
  }

  @Override
  public final String toString() {
    return ExpressionFormatter.formatExpression(this);
  }

  public TableExpressionType getExpressionType() {
    throw new UnsupportedOperationException(
        "getExpressionType is not implemented yet: " + this.getClass().getSimpleName());
  }

  // TODO make abstract later
  protected void serialize(ByteBuffer byteBuffer) {}

  // TODO make abstract later
  protected void serialize(DataOutputStream stream) throws IOException {}

  public static void serialize(Expression expression, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(
        expression.getExpressionType().getExpressionTypeInShortEnum(), byteBuffer);

    expression.serialize(byteBuffer);
  }

  public static void serialize(Expression expression, DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(expression.getExpressionType().getExpressionTypeInShortEnum(), stream);
    expression.serialize(stream);
  }

  public static Expression deserialize(ByteBuffer byteBuffer) {
    short type = ReadWriteIOUtils.readShort(byteBuffer);

    Expression expression = null;
    switch (type) {
      case 1:
        expression = new ArithmeticBinaryExpression(byteBuffer);
        break;
      case 2:
        expression = new ArithmeticUnaryExpression(byteBuffer);
        break;
      case 3:
        expression = new LikePredicate(byteBuffer);
        break;
      case 4:
        expression = new InListExpression(byteBuffer);
        break;
      case 5:
        expression = new IsNotNullPredicate(byteBuffer);
        break;
      case 6:
        expression = new IsNullPredicate(byteBuffer);
        break;
      case 7:
        expression = new FunctionCall(byteBuffer);
        break;
      case 8:
        expression = new Identifier(byteBuffer);
        break;
      case 9:
        expression = new Cast(byteBuffer);
        break;
      case 10:
        expression = new GenericDataType(byteBuffer);
        break;
      case 11:
        expression = new BetweenPredicate(byteBuffer);
        break;
      case 12:
        expression = new InPredicate(byteBuffer);
        break;
      case 13:
        expression = new LogicalExpression(byteBuffer);
        break;
      case 14:
        expression = new NotExpression(byteBuffer);
        break;
      case 15:
        expression = new ComparisonExpression(byteBuffer);
        break;
      case 16:
        expression = new BinaryLiteral(byteBuffer);
        break;
      case 17:
        expression = new BooleanLiteral(byteBuffer);
        break;
      case 18:
        expression = new DecimalLiteral(byteBuffer);
        break;
      case 19:
        expression = new DoubleLiteral(byteBuffer);
        break;
      case 20:
        expression = new GenericLiteral(byteBuffer);
        break;
      case 21:
        expression = new LongLiteral(byteBuffer);
        break;
      case 22:
        expression = new NullLiteral(byteBuffer);
        break;
      case 23:
        expression = new StringLiteral(byteBuffer);
        break;
      case 24:
        expression = new SymbolReference(byteBuffer);
        break;
      case 25:
        expression = new CoalesceExpression(byteBuffer);
        break;
      case 26:
        expression = new SimpleCaseExpression(byteBuffer);
        break;
      case 27:
        expression = new SearchedCaseExpression(byteBuffer);
        break;
      case 28:
        expression = new WhenClause(byteBuffer);
        break;
      default:
        throw new IllegalArgumentException("Invalid expression type: " + type);
    }

    return expression;
  }
}
