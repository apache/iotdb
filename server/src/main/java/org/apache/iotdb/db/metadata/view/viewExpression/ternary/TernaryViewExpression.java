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

package org.apache.iotdb.db.metadata.view.viewExpression.ternary;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public abstract class TernaryViewExpression extends ViewExpression {

  // region member variables and init functions
  protected final ViewExpression firstExpression;

  protected final ViewExpression secondExpression;

  protected final ViewExpression thirdExpression;

  protected TernaryViewExpression(
      ViewExpression firstExpression,
      ViewExpression secondExpression,
      ViewExpression thirdExpression) {
    this.firstExpression = firstExpression;
    this.secondExpression = secondExpression;
    this.thirdExpression = thirdExpression;
  }

  protected TernaryViewExpression(ByteBuffer byteBuffer) {
    this.firstExpression = ViewExpression.deserialize(byteBuffer);
    this.secondExpression = ViewExpression.deserialize(byteBuffer);
    this.thirdExpression = ViewExpression.deserialize(byteBuffer);
  }

  protected TernaryViewExpression(InputStream inputStream) {
    this.firstExpression = ViewExpression.deserialize(inputStream);
    this.secondExpression = ViewExpression.deserialize(inputStream);
    this.thirdExpression = ViewExpression.deserialize(inputStream);
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitTernaryExpression(this, context);
  }

  @Override
  protected final boolean isLeafOperandInternal() {
    return false;
  }

  @Override
  public final List<ViewExpression> getChildViewExpressions() {
    return Arrays.asList(firstExpression, secondExpression, thirdExpression);
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ViewExpression.serialize(firstExpression, byteBuffer);
    ViewExpression.serialize(secondExpression, byteBuffer);
    ViewExpression.serialize(thirdExpression, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    ViewExpression.serialize(firstExpression, stream);
    ViewExpression.serialize(secondExpression, stream);
    ViewExpression.serialize(thirdExpression, stream);
  }
  // endregion

  public ViewExpression getFirstExpression() {
    return this.firstExpression;
  }

  public ViewExpression getSecondExpression() {
    return this.secondExpression;
  }

  public ViewExpression getThirdExpression() {
    return this.thirdExpression;
  }
}
