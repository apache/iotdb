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
package org.apache.iotdb.db.query.expression;

public enum ExpressionType {
  ADDITION((short) 0),
  DIVISION((short) 1),
  EQUAL_TO((short) 2),
  GREATER_EQUAL((short) 3),
  GREATER_THAN((short) 4),
  LESS_EQUAL((short) 5),
  LESS_THAN((short) 6),
  LOGIC_AND((short) 7),
  LOGIC_OR((short) 8),
  MODULO((short) 9),
  MULTIPLICATION((short) 10),
  NON_EQUAL((short) 11),
  SUBTRACTION((short) 12),
  FUNCTION((short) 13),
  LOGIC_NOT((short) 14),
  NEGATION((short) 15),
  TIME_SERIES((short) 16),
  CONSTANT((short) 17),
  IN((short) 18),
  REGULAR((short) 19);

  private final short expressionType;

  ExpressionType(short expressionType) {
    this.expressionType = expressionType;
  }

  public short getExpressionType() {
    return expressionType;
  }
}
