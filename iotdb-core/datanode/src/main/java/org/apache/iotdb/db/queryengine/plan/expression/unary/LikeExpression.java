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

package org.apache.iotdb.db.queryengine.plan.expression.unary;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class LikeExpression extends UnaryExpression {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LikeExpression.class);

  private final String patternString;
  private final Optional<String> escape;

  private final boolean isNot;

  public LikeExpression(
      Expression expression, String patternString, Optional<String> escape, boolean isNot) {
    super(expression);
    this.patternString = patternString;
    this.escape = escape;
    this.isNot = isNot;
  }

  public LikeExpression(Expression expression, String patternString, boolean isNot) {
    super(expression);
    this.patternString = patternString;
    this.escape = Optional.empty();
    this.isNot = isNot;
  }

  public LikeExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      escape = Optional.ofNullable(ReadWriteIOUtils.readString(byteBuffer));
    } else {
      escape = Optional.empty();
    }
    isNot = ReadWriteIOUtils.readBool(byteBuffer);
  }

  public String getPatternString() {
    return patternString;
  }

  public Optional<String> getEscape() {
    return escape;
  }

  public boolean isNot() {
    return isNot;
  }

  @Override
  protected String getExpressionStringInternal() {
    String res =
        expression.getExpressionString()
            + (isNot ? " NOT" : "")
            + " LIKE "
            + "pattern = '"
            + patternString
            + "'";
    if (escape.isPresent()) {
      res = res + " escape = '" + escape + "'";
    }
    return res;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.LIKE;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(patternString, byteBuffer);
    ReadWriteIOUtils.write(escape.isPresent(), byteBuffer);
    if (escape.isPresent()) {
      ReadWriteIOUtils.write(String.valueOf(escape), byteBuffer);
    }
    ReadWriteIOUtils.write(isNot, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(patternString, stream);
    ReadWriteIOUtils.write(escape.isPresent(), stream);
    if (escape.isPresent()) {
      ReadWriteIOUtils.write(String.valueOf(escape), stream);
    }
    ReadWriteIOUtils.write(isNot, stream);
  }

  @Override
  public String getOutputSymbolInternal() {
    String res =
        expression.getOutputSymbol()
            + (isNot ? " NOT" : "")
            + " LIKE "
            + "pattern = '"
            + patternString
            + "'";
    if (escape.isPresent()) {
      res = res + " escape = '" + escape + "'";
    }
    return res;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLikeExpression(this, context);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(expression)
        + RamUsageEstimator.sizeOf(patternString);
  }
}
