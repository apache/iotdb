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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

public abstract class Node {

  @Nullable private final NodeLocation location;

  protected Node(@Nullable NodeLocation location) {
    this.location = location;
  }

  /** Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead. */
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

  public Optional<NodeLocation> getLocation() {
    return Optional.ofNullable(location);
  }

  public abstract List<? extends Node> getChildren();

  // Force subclasses to have a proper equals and hashcode implementation
  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  /**
   * Compare with another node by considering internal state excluding any Node returned by
   * getChildren()
   */
  public boolean shallowEquals(Node other) {
    throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
  }

  public static boolean sameClass(Node left, Node right) {
    if (left == right) {
      return true;
    }

    return left.getClass() == right.getClass();
  }
}
