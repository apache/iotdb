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

package org.apache.iotdb.db.query.expression.binary;

import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class BinaryExpression extends Expression {

  protected BinaryExpression(Expression leftExpression, Expression rightExpression) {
    this.leftExpression = leftExpression;
    this.rightExpression = rightExpression;
  }

  protected final Expression leftExpression;
  protected final Expression rightExpression;

  /**
   * The result data type of all arithmetic operations will be DOUBLE.
   *
   * <p>TODO: This is just a simple implementation and should be optimized later.
   */
  @Override
  public TSDataType dataType() {
    return TSDataType.DOUBLE;
  }

  @Override
  public final String toString() {
    return String.format(
        "%s %s %s", leftExpression.toString(), operator(), rightExpression.toString());
  }

  protected abstract String operator();
}
