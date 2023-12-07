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

package org.apache.iotdb.tsfile.read.filter.basic;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/* base class for And, Or */
public abstract class BinaryLogicalFilter extends Filter {

  protected final Filter left;
  protected final Filter right;

  protected BinaryLogicalFilter(Filter left, Filter right) {
    this.left = Objects.requireNonNull(left, "left cannot be null");
    this.right = Objects.requireNonNull(right, "right cannot be null");
  }

  public Filter getLeft() {
    return left;
  }

  public Filter getRight() {
    return right;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    super.serialize(outputStream);
    left.serialize(outputStream);
    right.serialize(outputStream);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BinaryLogicalFilter that = (BinaryLogicalFilter) o;
    return left.equals(that.left) && right.equals(that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right);
  }
}
