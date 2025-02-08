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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

public enum TableExpressionType {
  ARITHMETIC_BINARY((short) 1),
  ARITHMETIC_UNARY((short) 2),
  LIKE_PREDICATE((short) 3),
  IN_LIST((short) 4),
  IS_NOT_NULL_PREDICATE((short) 5),
  IS_NULL_PREDICATE((short) 6),
  FUNCTION_CALL((short) 7),
  IDENTIFIER((short) 8),
  CAST((short) 9),
  GENERIC_DATA_TYPE((short) 10),
  BETWEEN((short) 11),
  IN_PREDICATE((short) 12),
  LOGICAL_EXPRESSION((short) 13),
  NOT_EXPRESSION((short) 14),
  COMPARISON((short) 15),
  BINARY_LITERAL((short) 16),
  BOOLEAN_LITERAL((short) 17),
  DECIMAL_LITERAL((short) 18),
  DOUBLE_LITERAL((short) 19),
  GENERIC_LITERAL((short) 20),
  LONG_LITERAL((short) 21),
  NULL_LITERAL((short) 22),
  STRING_LITERAL((short) 23),
  SYMBOL_REFERENCE((short) 24),
  COALESCE((short) 25),
  SIMPLE_CASE((short) 26),
  SEARCHED_CASE((short) 27),
  WHEN_CLAUSE((short) 28),
  CURRENT_DATABASE((short) 29),
  CURRENT_USER((short) 30);

  TableExpressionType(short type) {
    this.type = type;
  }

  private final short type;

  public short getExpressionTypeInShortEnum() {
    return type;
  }
}
