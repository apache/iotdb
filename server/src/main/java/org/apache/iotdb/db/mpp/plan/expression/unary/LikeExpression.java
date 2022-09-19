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
import java.util.regex.Pattern;

public class LikeExpression extends UnaryExpression {

  private final String patternString;
  private final Pattern pattern;

  public LikeExpression(Expression expression, String patternString) {
    super(expression);
    this.patternString = patternString;
    pattern = compile();
  }

  public LikeExpression(Expression expression, String patternString, Pattern pattern) {
    super(expression);
    this.patternString = patternString;
    this.pattern = pattern;
  }

  public LikeExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    pattern = compile();
  }

  public String getPatternString() {
    return patternString;
  }

  public Pattern getPattern() {
    return pattern;
  }

  /**
   * The main idea of this part comes from
   * https://codereview.stackexchange.com/questions/36861/convert-sql-like-to-regex/36864
   */
  private Pattern compile() {
    String unescapeValue = unescapeString(patternString);
    String specialRegexString = ".^$*+?{}[]|()";
    StringBuilder patternBuilder = new StringBuilder();
    patternBuilder.append("^");
    for (int i = 0; i < unescapeValue.length(); i++) {
      String ch = String.valueOf(unescapeValue.charAt(i));
      if (specialRegexString.contains(ch)) {
        ch = "\\" + unescapeValue.charAt(i);
      }
      if (i == 0
          || !"\\".equals(String.valueOf(unescapeValue.charAt(i - 1)))
          || i >= 2
              && "\\\\"
                  .equals(
                      patternBuilder.substring(
                          patternBuilder.length() - 2, patternBuilder.length()))) {
        patternBuilder.append(ch.replace("%", ".*?").replace("_", "."));
      } else {
        patternBuilder.append(ch);
      }
    }
    patternBuilder.append("$");
    return Pattern.compile(patternBuilder.toString());
  }

  /**
   * This Method is for un-escaping strings except '\' before special string '%', '_', '\', because
   * we need to use '\' to judge whether to replace this to regexp string
   */
  private String unescapeString(String value) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      String ch = String.valueOf(value.charAt(i));
      if ("\\".equals(ch)) {
        if (i < value.length() - 1) {
          String nextChar = String.valueOf(value.charAt(i + 1));
          if ("%".equals(nextChar) || "_".equals(nextChar) || "\\".equals(nextChar)) {
            stringBuilder.append(ch);
          }
          if ("\\".equals(nextChar)) {
            i++;
          }
        }
      } else {
        stringBuilder.append(ch);
      }
    }
    return stringBuilder.toString();
  }

  @Override
  protected String getExpressionStringInternal() {
    return expression + " LIKE '" + pattern + "'";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.LIKE;
  }

  @Override
  protected Expression constructExpression(Expression childExpression) {
    return new LikeExpression(childExpression, patternString, pattern);
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
    return visitor.visitLikeExpression(this, context);
  }
}
