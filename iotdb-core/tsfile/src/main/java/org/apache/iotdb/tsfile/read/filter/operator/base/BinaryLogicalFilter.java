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

package org.apache.iotdb.tsfile.read.filter.operator.base;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.Locale;
import java.util.Objects;

/* base class for And, Or */
public abstract class BinaryLogicalFilter {

  protected final Filter left;
  protected final Filter right;

  private final String toString;

  protected BinaryLogicalFilter(Filter left, Filter right) {
    this.left = Objects.requireNonNull(left, "left cannot be null");
    this.right = Objects.requireNonNull(right, "right cannot be null");

    String name = getClass().getSimpleName().toLowerCase(Locale.ENGLISH);
    this.toString = name + "(" + left + ", " + right + ")";
  }

  public Filter getLeft() {
    return left;
  }

  public Filter getRight() {
    return right;
  }

  @Override
  public String toString() {
    return toString;
  }
}
