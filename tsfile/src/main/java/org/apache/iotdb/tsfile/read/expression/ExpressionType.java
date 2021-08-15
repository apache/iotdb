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
package org.apache.iotdb.tsfile.read.expression;

public enum ExpressionType {

  /** Represent the relationship between the left expression and the right expression is AND */
  AND,

  /** Represent the relationship between the left expression and the right expression is OR */
  OR,

  /**
   * Represents that the expression is a leaf node in the expression tree and the type is value
   * filtering
   */
  SERIES,

  /**
   * Represents that the expression is a leaf node in the expression tree and the type is time
   * filtering
   */
  GLOBAL_TIME,

  /**
   * This type is used in the pruning process of expression tree in the distributed reading process.
   * When pruning a expression tree for a data group, leaf nodes belonging to other data groups will
   * be set to that type.
   */
  TRUE
}
