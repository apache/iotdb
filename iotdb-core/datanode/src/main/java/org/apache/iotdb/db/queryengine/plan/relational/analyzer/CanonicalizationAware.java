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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Node;

import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil.treeEqual;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil.treeHash;

public class CanonicalizationAware<T extends Node> {

  private final T node;

  // Updates to this field are thread-safe despite benign data race due to:
  // 1. idempotent hash computation
  // 2. atomic updates to int fields per JMM
  private int hashCode;

  private CanonicalizationAware(T node) {
    this.node = requireNonNull(node, "node is null");
  }

  public static <T extends Node> CanonicalizationAware<T> canonicalizationAwareKey(T node) {
    return new CanonicalizationAware<T>(node);
  }

  public T getNode() {
    return node;
  }

  @Override
  public int hashCode() {
    int hash = hashCode;
    if (hash == 0) {
      hash = treeHash(node, CanonicalizationAware::canonicalizationAwareHash);
      if (hash == 0) {
        hash = 1;
      }

      hashCode = hash;
    }

    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CanonicalizationAware<T> other = (CanonicalizationAware<T>) o;
    return treeEqual(node, other.node, CanonicalizationAware::canonicalizationAwareComparison);
  }

  @Override
  public String toString() {
    return "CanonicalizationAware(" + node + ")";
  }

  public static Boolean canonicalizationAwareComparison(Node left, Node right) {
    if (left instanceof Identifier && right instanceof Identifier) {
      Identifier leftIdentifier = (Identifier) left;
      Identifier rightIdentifier = (Identifier) right;
      return leftIdentifier.getCanonicalValue().equals(rightIdentifier.getCanonicalValue());
    }

    return null;
  }

  public static OptionalInt canonicalizationAwareHash(Node node) {
    if (node instanceof Identifier) {
      Identifier identifier = (Identifier) node;
      return OptionalInt.of(identifier.getCanonicalValue().hashCode());
    }
    if (node.getChildren().isEmpty()) {
      return OptionalInt.of(node.hashCode());
    }
    return OptionalInt.empty();
  }
}
