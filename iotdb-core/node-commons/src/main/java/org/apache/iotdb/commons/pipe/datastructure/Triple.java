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

package org.apache.iotdb.commons.pipe.datastructure;

import java.util.Objects;

public class Triple<L, M, R> {
  public final L first;
  public final M second;
  public final R third;

  public Triple(final L first, final M second, final R third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  public L getFirst() {
    return first;
  }

  public M getSecond() {
    return second;
  }

  public R getThird() {
    return third;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
    return first.equals(triple.first) && second.equals(triple.second) && third.equals(triple.third);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second, third);
  }
}
