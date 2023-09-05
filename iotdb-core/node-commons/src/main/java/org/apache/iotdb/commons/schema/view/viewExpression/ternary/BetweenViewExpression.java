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

package org.apache.iotdb.commons.schema.view.viewExpression.ternary;

import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BetweenViewExpression extends TernaryViewExpression {

  // region member variables and init functions
  /** default value is false. */
  private final boolean isNotBetween;

  public BetweenViewExpression(
      ViewExpression firstExpression,
      ViewExpression secondExpression,
      ViewExpression thirdExpression,
      boolean isNotBetween) {
    super(firstExpression, secondExpression, thirdExpression);
    this.isNotBetween = isNotBetween;
  }

  public BetweenViewExpression(
      ViewExpression firstExpression,
      ViewExpression secondExpression,
      ViewExpression thirdExpression) {
    super(firstExpression, secondExpression, thirdExpression);
    this.isNotBetween = false;
  }

  public BetweenViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
    this.isNotBetween = ReadWriteIOUtils.readBool(byteBuffer);
  }

  public BetweenViewExpression(InputStream inputStream) {
    super(inputStream);
    try {
      this.isNotBetween = ReadWriteIOUtils.readBool(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitBetweenExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.BETWEEN;
  }

  @Override
  public String toString(boolean isRoot) {
    String basicString =
        this.firstExpression.toString(true)
            + " BETWEEN "
            + this.secondExpression.toString(false)
            + " AND "
            + this.secondExpression.toString(false);
    if (isRoot) {
      return basicString;
    }
    return "(" + basicString + ")";
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isNotBetween, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(isNotBetween, stream);
  }
  // endregion

  public boolean isNotBetween() {
    return isNotBetween;
  }
}
