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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression;

import java.util.List;

public class BinaryComputation extends Computation {
  private final Computation left;
  private final Computation right;
  private final BinaryOperator operator;

  public BinaryComputation(Computation left, Computation right, BinaryOperator operator) {
    this.left = left;
    this.right = right;
    this.operator = operator;
  }

  @Override
  public Object evaluate(List<Object> values) {
    Object leftValue = left.evaluate(values);
    Object rightValue = right.evaluate(values);
    return operator.apply(leftValue, rightValue);
  }
}
