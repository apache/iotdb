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

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.Validate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import static org.apache.iotdb.tsfile.utils.RegexUtils.compileRegex;

public class RegularExpression extends UnaryExpression {

  private final String patternString;
  private final Pattern pattern;

  private final boolean isNot;

  public RegularExpression(Expression expression, String patternString, boolean isNot) {
    super(expression);
    this.patternString = patternString;
    this.isNot = isNot;
    pattern = compileRegex(patternString);
  }

  public RegularExpression(
      Expression expression, String patternString, Pattern pattern, boolean isNot) {
    super(expression);
    this.patternString = patternString;
    this.pattern = pattern;
    this.isNot = isNot;
  }

  public RegularExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    isNot = ReadWriteIOUtils.readBool(byteBuffer);
    pattern = compileRegex(Validate.notNull(patternString, "patternString cannot be null"));
  }

  public String getPatternString() {
    return patternString;
  }

  public Pattern getPattern() {
    return pattern;
  }

  public boolean isNot() {
    return isNot;
  }

  @Override
  protected String getExpressionStringInternal() {
    return expression.getExpressionString()
        + (isNot ? " NOT" : "")
        + " REGEXP '"
        + patternString
        + "'";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.REGEXP;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(patternString, byteBuffer);
    ReadWriteIOUtils.write(isNot, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(patternString, stream);
    ReadWriteIOUtils.write(isNot, stream);
  }

  @Override
  public String getOutputSymbolInternal() {
    return expression.getOutputSymbol() + (isNot ? " NOT" : "") + " REGEXP '" + patternString + "'";
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitRegularExpression(this, context);
  }
}
