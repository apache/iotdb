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
import java.util.regex.Pattern;

/* base class for Regex, NotRegex */
public abstract class ColumnPatternMatchFilter {

  protected final Pattern pattern;

  protected ColumnPatternMatchFilter(Pattern pattern) {
    this.pattern = Objects.requireNonNull(pattern, "pattern cannot be null");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnPatternMatchFilter that = (ColumnPatternMatchFilter) o;
    return pattern.pattern().equals(that.pattern.pattern());
  }

  @Override
  public int hashCode() {
    return Objects.hash(pattern.pattern());
  }
}
