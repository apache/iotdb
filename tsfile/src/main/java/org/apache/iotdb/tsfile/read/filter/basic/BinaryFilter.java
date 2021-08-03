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

import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Definition for binary filter operations. */
public abstract class BinaryFilter implements Filter, Serializable {

  private static final long serialVersionUID = 1039585564327602465L;

  protected Filter left;
  protected Filter right;

  public BinaryFilter() {}

  protected BinaryFilter(Filter left, Filter right) {
    this.left = left;
    this.right = right;
  }

  public Filter getLeft() {
    return left;
  }

  public Filter getRight() {
    return right;
  }

  @Override
  public String toString() {
    return "( " + left + "," + right + " )";
  }

  @Override
  public abstract Filter copy();

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      left.serialize(outputStream);
      right.serialize(outputStream);
    } catch (IOException ignored) {
      // ignore
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    left = FilterFactory.deserialize(buffer);
    right = FilterFactory.deserialize(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof BinaryFilter)) {
      return false;
    }
    BinaryFilter other = ((BinaryFilter) obj);
    return this.left.equals(other.left)
        && this.right.equals(other.right)
        && this.getSerializeId().equals(other.getSerializeId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right, getSerializeId());
  }
}
