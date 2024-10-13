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

package org.apache.iotdb.commons.schema.view.viewExpression.unary;

import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

public class LikeViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  private final String pattern;
  private final Optional<Character> escape;
  private final boolean isNot;

  public LikeViewExpression(
      ViewExpression expression, String pattern, Optional<Character> escape, boolean isNot) {
    super(expression);
    this.pattern = pattern;
    this.escape = escape;
    this.isNot = isNot;
  }

  public LikeViewExpression(ViewExpression expression, String pattern, boolean isNot) {
    super(expression);
    this.pattern = pattern;
    this.escape = Optional.empty();
    this.isNot = isNot;
  }

  public LikeViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
    pattern = ReadWriteIOUtils.readString(byteBuffer);
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      escape = Optional.of(ReadWriteIOUtils.readString(byteBuffer).charAt(0));
    } else {
      escape = Optional.empty();
    }
    isNot = ReadWriteIOUtils.readBool(byteBuffer);
  }

  public LikeViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
    try {
      pattern = ReadWriteIOUtils.readString(inputStream);
      if (ReadWriteIOUtils.readBool(inputStream)) {
        escape = Optional.of(ReadWriteIOUtils.readString(inputStream).charAt(0));
      } else {
        escape = Optional.empty();
      }
      isNot = ReadWriteIOUtils.readBool(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLikeExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LIKE;
  }

  @Override
  public String toString(boolean isRoot) {
    String basicString =
        this.expression.toString(false)
            + (isNot ? "NOT LIKE " : "LIKE ")
            + "pattern = '"
            + this.pattern
            + "'";
    if (escape.isPresent()) {
      basicString += " escape = '" + escape.get() + "'";
    }
    if (isRoot) {
      return basicString;
    }
    return "( " + basicString + " )";
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(pattern, byteBuffer);
    ReadWriteIOUtils.write(escape.isPresent(), byteBuffer);
    if (escape.isPresent()) {
      ReadWriteIOUtils.write(escape.get().toString(), byteBuffer);
    }
    ReadWriteIOUtils.write(isNot, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(pattern, stream);
    ReadWriteIOUtils.write(escape.isPresent(), stream);
    if (escape.isPresent()) {
      ReadWriteIOUtils.write(escape.get().toString(), stream);
    }
    ReadWriteIOUtils.write(isNot, stream);
  }

  // endregion

  public String getPattern() {
    return pattern;
  }

  public Optional<Character> getEscape() {
    return escape;
  }

  public boolean isNot() {
    return isNot;
  }
}
