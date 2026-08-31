/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.alwaysFalse;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup.noLookup;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.ChildReplacer.replaceChildren;

public class PlanNodeSearcher {
  public static PlanNodeSearcher searchFrom(PlanNode node) {
    return searchFrom(node, noLookup());
  }

  /**
   * Use it in optimizer {@link
   * org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule} only if you truly do
   * not have a better option
   *
   * <p>TODO: replace it with a support for plan (physical) properties in rules pattern matching
   */
  public static PlanNodeSearcher searchFrom(PlanNode node, Lookup lookup) {
    return new PlanNodeSearcher(node, lookup);
  }

  private final PlanNode node;
  private final Lookup lookup;
  private Predicate<PlanNode> where = alwaysTrue();
  private Predicate<PlanNode> recurseOnlyWhen = alwaysTrue();

  private PlanNodeSearcher(PlanNode node, Lookup lookup) {
    this.node = requireNonNull(node, DataNodeQueryMessages.EXCEPTION_NODE_IS_NULL_C1479F4A);
    this.lookup = requireNonNull(lookup, DataNodeQueryMessages.EXCEPTION_LOOKUP_IS_NULL_B8FD7E65);
  }

  @SafeVarargs
  public final PlanNodeSearcher whereIsInstanceOfAny(Class<? extends PlanNode>... classes) {
    return whereIsInstanceOfAny(asList(classes));
  }

  public final PlanNodeSearcher whereIsInstanceOfAny(List<Class<? extends PlanNode>> classes) {
    Predicate<PlanNode> predicate = alwaysFalse();
    for (Class<?> clazz : classes) {
      predicate = predicate.or(clazz::isInstance);
    }
    return where(predicate);
  }

  public PlanNodeSearcher where(Predicate<PlanNode> where) {
    this.where = requireNonNull(where, DataNodeQueryMessages.EXCEPTION_WHERE_IS_NULL_A1A3FCBC);
    return this;
  }

  public PlanNodeSearcher recurseOnlyWhen(Predicate<PlanNode> skipOnly) {
    this.recurseOnlyWhen =
        requireNonNull(skipOnly, DataNodeQueryMessages.EXCEPTION_SKIPONLY_IS_NULL_80DB0703);
    return this;
  }

  public <T extends PlanNode> Optional<T> findFirst() {
    return findFirstRecursive(node);
  }

  private <T extends PlanNode> Optional<T> findFirstRecursive(PlanNode node) {
    node = lookup.resolve(node);

    if (where.test(node)) {
      return Optional.of((T) node);
    }
    if (recurseOnlyWhen.test(node)) {
      for (PlanNode source : node.getChildren()) {
        Optional<T> found = findFirstRecursive(source);
        if (found.isPresent()) {
          return found;
        }
      }
    }
    return Optional.empty();
  }

  /** Return a list of matching nodes ordered as in pre-order traversal of the plan tree. */
  public <T extends PlanNode> List<T> findAll() {
    ImmutableList.Builder<T> nodes = ImmutableList.builder();
    findAllRecursive(node, nodes);
    return nodes.build();
  }

  public <T extends PlanNode> T findOnlyElement() {
    return getOnlyElement(findAll());
  }

  private <T extends PlanNode> void findAllRecursive(
      PlanNode node, ImmutableList.Builder<T> nodes) {
    node = lookup.resolve(node);

    if (where.test(node)) {
      nodes.add((T) node);
    }
    if (recurseOnlyWhen.test(node)) {
      for (PlanNode source : node.getChildren()) {
        findAllRecursive(source, nodes);
      }
    }
  }

  public PlanNode removeAll() {
    return removeAllRecursive(node);
  }

  private PlanNode removeAllRecursive(PlanNode node) {
    node = lookup.resolve(node);

    if (where.test(node)) {
      checkArgument(
          node.getChildren().size() == 1,
          DataNodeQueryMessages
              .EXCEPTION_UNABLE_TO_REMOVE_PLAN_NODE_AS_IT_CONTAINS_0_OR_MORE_THAN_1_CHILDREN_6F26E194);
      return node.getChildren().get(0);
    }
    if (recurseOnlyWhen.test(node)) {
      List<PlanNode> sources =
          node.getChildren().stream().map(this::removeAllRecursive).collect(toImmutableList());
      return replaceChildren(node, sources);
    }
    return node;
  }

  public PlanNode removeFirst() {
    return removeFirstRecursive(node);
  }

  private PlanNode removeFirstRecursive(PlanNode node) {
    node = lookup.resolve(node);

    if (where.test(node)) {
      checkArgument(
          node.getChildren().size() == 1,
          DataNodeQueryMessages
              .EXCEPTION_UNABLE_TO_REMOVE_PLAN_NODE_AS_IT_CONTAINS_0_OR_MORE_THAN_1_CHILDREN_6F26E194);
      return node.getChildren().get(0);
    }
    if (recurseOnlyWhen.test(node)) {
      List<PlanNode> sources = node.getChildren();
      if (sources.isEmpty()) {
        return node;
      }
      if (sources.size() == 1) {
        return replaceChildren(node, ImmutableList.of(removeFirstRecursive(sources.get(0))));
      }
      throw new IllegalArgumentException(
          DataNodeQueryMessages
              .QUERY_EXCEPTION_UNABLE_TO_REMOVE_FIRST_NODE_WHEN_A_NODE_HAS_MULTIPLE_CHILDREN_FB6E81C5);
    }
    return node;
  }

  public PlanNode replaceAll(PlanNode newPlanNode) {
    return replaceAllRecursive(node, newPlanNode);
  }

  private PlanNode replaceAllRecursive(PlanNode node, PlanNode nodeToReplace) {
    node = lookup.resolve(node);

    if (where.test(node)) {
      return nodeToReplace;
    }
    if (recurseOnlyWhen.test(node)) {
      List<PlanNode> sources =
          node.getChildren().stream()
              .map(source -> replaceAllRecursive(source, nodeToReplace))
              .collect(toImmutableList());
      return replaceChildren(node, sources);
    }
    return node;
  }

  public PlanNode replaceFirst(PlanNode newPlanNode) {
    return replaceFirstRecursive(node, newPlanNode);
  }

  private PlanNode replaceFirstRecursive(PlanNode node, PlanNode nodeToReplace) {
    node = lookup.resolve(node);

    if (where.test(node)) {
      return nodeToReplace;
    }
    List<PlanNode> sources = node.getChildren();
    if (sources.isEmpty()) {
      return node;
    }
    if (sources.size() == 1) {
      return replaceChildren(node, ImmutableList.of(replaceFirstRecursive(node, sources.get(0))));
    }
    throw new IllegalArgumentException(
        DataNodeQueryMessages
            .QUERY_EXCEPTION_UNABLE_TO_REPLACE_FIRST_NODE_WHEN_A_NODE_HAS_MULTIPLE_CHILDREN_2C3D0E9E);
  }

  public boolean matches() {
    return findFirst().isPresent();
  }

  public int count() {
    return findAll().size();
  }
}
