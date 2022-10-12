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

import org.apache.commons.lang3.Validate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public class RegularExpression extends UnaryExpression {

  private final String patternString;
  private final Pattern pattern;

  public RegularExpression(Expression expression, String patternString) {
    super(expression);
    this.patternString = patternString;
    pattern = Pattern.compile(patternString);
  }

  public RegularExpression(Expression expression, String patternString, Pattern pattern) {
    super(expression);
    this.patternString = patternString;
    this.pattern = pattern;
  }

  public RegularExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    pattern = Pattern.compile(Validate.notNull(patternString));
  }

  public String getPatternString() {
    return patternString;
  }

  public Pattern getPattern() {
    return pattern;
  }

  @Override
  protected Expression constructExpression(Expression childExpression) {
    return new RegularExpression(childExpression, patternString, pattern);
  }

  @Override
  protected String getExpressionStringInternal() {
    return expression + " REGEXP '" + patternString + "'";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.REGEXP;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(patternString, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(patternString, stream);
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitRegularExpression(this, context);
  }
}
