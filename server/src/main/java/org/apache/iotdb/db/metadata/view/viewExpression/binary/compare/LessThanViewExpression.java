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

package org.apache.iotdb.db.metadata.view.viewExpression.binary.compare;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class LessThanViewExpression extends CompareBinaryViewExpression {

  public LessThanViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public LessThanViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public LessThanViewExpression(InputStream inputStream) {
    super(inputStream);
  }

  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLessThanExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "<";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LESS_THAN;
  }
}
