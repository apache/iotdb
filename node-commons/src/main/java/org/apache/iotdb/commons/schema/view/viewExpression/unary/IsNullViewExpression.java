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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class IsNullViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  private final boolean isNot;

  public IsNullViewExpression(ViewExpression expression, boolean isNot) {
    super(expression);
    this.isNot = isNot;
  }

  public IsNullViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
    isNot = ReadWriteIOUtils.readBool(byteBuffer);
  }

  public IsNullViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
    try {
      isNot = ReadWriteIOUtils.readBool(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitIsNullExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.IS_NULL;
  }

  @Override
  public String toString(boolean isRoot) {
    return this.expression.toString(false) + " IS_NULL";
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isNot, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(isNot, stream);
  }
  // endregion

  public boolean isNot() {
    return isNot;
  }
}
