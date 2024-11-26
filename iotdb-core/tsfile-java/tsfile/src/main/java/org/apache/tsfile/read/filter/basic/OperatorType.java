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

package org.apache.tsfile.read.filter.basic;

public enum OperatorType {
  // comparison
  TIME_EQ("="),
  VALUE_EQ("="),
  TIME_NEQ("!="),
  VALUE_NEQ("!="),
  TIME_GT(">"),
  VALUE_GT(">"),
  TIME_GTEQ(">="),
  VALUE_GTEQ(">="),
  TIME_LT("<"),
  VALUE_LT("<"),
  TIME_LTEQ("<="),
  VALUE_LTEQ("<="),

  // logical
  NOT("NOT"),
  AND("&&"),
  OR("||"),

  // range
  TIME_BETWEEN_AND("BETWEEN"),
  VALUE_BETWEEN_AND("BETWEEN"),
  TIME_NOT_BETWEEN_AND("NOT BETWEEN"),
  VALUE_NOT_BETWEEN_AND("NOT BETWEEN"),

  // set
  TIME_IN("IN"),
  VALUE_IN("IN"),
  TIME_NOT_IN("NOT IN"),
  VALUE_NOT_IN("NOT IN"),

  // regexp pattern match
  VALUE_REGEXP("MATCH"),
  VALUE_NOT_REGEXP("NOT MATCH"),

  // like pattern match
  VALUE_LIKE("MATCH"),
  VALUE_NOT_LIKE("NOT MATCH"),

  // group by
  GROUP_BY_TIME("GROUP BY TIME"),
  GROUP_BY_MONTH("GROUP BY MONTH"),

  // is null
  VALUE_IS_NULL("IS NULL"),
  VALUE_IS_NOT_NULL("IS NOT NULL");

  private final String symbol;

  OperatorType(String symbol) {
    this.symbol = symbol;
  }

  public String getSymbol() {
    return symbol;
  }
}
