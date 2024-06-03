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
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public abstract class UnaryViewExpression extends ViewExpression {

  // region member variables and init functions
  protected final ViewExpression expression;

  protected UnaryViewExpression(ViewExpression expression) {
    this.expression = expression;
  }

  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitUnaryExpression(this, context);
  }

  @Override
  protected final boolean isLeafOperandInternal() {
    return false;
  }

  @Override
  public final List<ViewExpression> getChildViewExpressions() {
    // leaf node has no child nodes.
    return Arrays.asList(expression);
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ViewExpression.serialize(expression, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    ViewExpression.serialize(expression, stream);
  }

  // endregion

  public final ViewExpression getExpression() {
    return expression;
  }
}
