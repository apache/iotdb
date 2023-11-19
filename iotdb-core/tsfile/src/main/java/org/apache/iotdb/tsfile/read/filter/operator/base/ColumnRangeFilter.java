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

import java.util.Objects;

/* base class for BetweenAnd, NotBetweenAnd */
public abstract class ColumnRangeFilter<T extends Comparable<T>> {

  protected final T min;
  protected final T max;

  protected ColumnRangeFilter(T min, T max) {
    this.min = Objects.requireNonNull(min, "min cannot be null");
    this.max = Objects.requireNonNull(max, "max cannot be null");
  }
}
