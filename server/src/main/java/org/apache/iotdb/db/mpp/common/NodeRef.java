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

package org.apache.iotdb.db.mpp.common;

import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public class NodeRef<T> {

  private final T node;

  public NodeRef(T node) {
    this.node = requireNonNull(node, "node is null");
  }

  public static <T> NodeRef<T> of(T node) {
    return new NodeRef<>(node);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeRef<?> other = (NodeRef<?>) o;
    return node == other.node;
  }

  @Override
  public int hashCode() {
    return identityHashCode(node);
  }

  @Override
  public String toString() {
    return format("@%s: %s", Integer.toHexString(identityHashCode(node)), node);
  }
}
