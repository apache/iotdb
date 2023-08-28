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

package org.apache.iotdb.commons.schema.view.viewExpression.binary;

import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public abstract class BinaryViewExpression extends ViewExpression {

  // region member variables and init functions
  protected ViewExpression leftExpression;
  protected ViewExpression rightExpression;

  protected BinaryViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    this.leftExpression = leftExpression;
    this.rightExpression = rightExpression;
  }

  protected BinaryViewExpression(ByteBuffer byteBuffer) {
    this.leftExpression = ViewExpression.deserialize(byteBuffer);
    this.rightExpression = ViewExpression.deserialize(byteBuffer);
  }

  protected BinaryViewExpression(InputStream inputStream) {
    this.leftExpression = ViewExpression.deserialize(inputStream);
    this.rightExpression = ViewExpression.deserialize(inputStream);
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitBinaryExpression(this, context);
  }

  @Override
  protected final boolean isLeafOperandInternal() {
    return false;
  }

  @Override
  public final List<ViewExpression> getChildViewExpressions() {
    return Arrays.asList(leftExpression, rightExpression);
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ViewExpression.serialize(leftExpression, byteBuffer);
    ViewExpression.serialize(rightExpression, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    ViewExpression.serialize(leftExpression, stream);
    ViewExpression.serialize(rightExpression, stream);
  }
  // endregion
  public void setLeftExpression(ViewExpression leftExpression) {
    this.leftExpression = leftExpression;
  }

  public void setRightExpression(ViewExpression rightExpression) {
    this.rightExpression = rightExpression;
  }

  public ViewExpression getLeftExpression() {
    return leftExpression;
  }

  public ViewExpression getRightExpression() {
    return rightExpression;
  }

  public abstract String getStringSymbol();

  @Override
  public String toString() {
    return this.toString(true);
  }

  @Override
  public String toString(boolean isRoot) {
    String basicString =
        leftExpression.toString(false)
            + " "
            + this.getStringSymbol()
            + " "
            + rightExpression.toString(false);
    if (isRoot) {
      return basicString;
    } else {
      return "(" + basicString + ")";
    }
  }
}
