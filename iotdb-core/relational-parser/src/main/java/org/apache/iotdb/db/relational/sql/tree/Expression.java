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

package org.apache.iotdb.db.relational.sql.tree;

import org.apache.iotdb.db.relational.sql.util.ExpressionFormatter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
    return null;
  }

  protected void serialize(ByteBuffer byteBuffer) {}

  protected void serialize(DataOutputStream stream) {}

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

    Expression expression;
    switch (type) {
      case 0:
        expression = new ComparisonExpression(byteBuffer);
        break;
      default:
        throw new IllegalArgumentException("Invalid expression type: " + type);
    }

    return expression;
  }
}
