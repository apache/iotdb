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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.PropertyPattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.transpose;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.groupNode;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

public class GatherAndMergeWindows {
  private GatherAndMergeWindows() {}

  public static Set<Rule<?>> rules() {
    return IntStream.range(0, 5)
        .boxed()
        .flatMap(
            numProjects ->
                Stream.of(
                    new MergeAdjacentWindowsOverProjects(numProjects),
                    new SwapAdjacentWindowsBySpecifications(numProjects)))
        .collect(toImmutableSet());
  }

  private abstract static class ManipulateAdjacentWindowsOverProjects implements Rule<WindowNode> {
    private final Capture<WindowNode> childCapture = newCapture();
    private final List<Capture<ProjectNode>> projectCaptures;
    private final Pattern<WindowNode> pattern;

    protected ManipulateAdjacentWindowsOverProjects(int numProjects) {
      PropertyPattern<PlanNode, ?, ?> childPattern =
          source().matching(window().with(source().matching(groupNode())).capturedAs(childCapture));
      ImmutableList.Builder<Capture<ProjectNode>> projectCapturesBuilder = ImmutableList.builder();
      for (int i = 0; i < numProjects; ++i) {
        Capture<ProjectNode> projectCapture = newCapture();
        projectCapturesBuilder.add(projectCapture);
        childPattern = source().matching(project().capturedAs(projectCapture).with(childPattern));
      }
      this.projectCaptures = projectCapturesBuilder.build();
      this.pattern = window().with(source().matching(groupNode().with(childPattern)));
    }

    @Override
    public Pattern<WindowNode> getPattern() {
      return pattern;
    }

    @Override
    public Result apply(WindowNode parent, Captures captures, Context context) {
      // Pulling the descendant WindowNode above projects is done as a part of this rule, as opposed
      // in a
      // separate rule, because that pullup is not useful on its own, and could be undone by other
      // rules.
      // For example, a rule could insert a project-off node between adjacent WindowNodes that use
      // different
      // input symbols.
      List<ProjectNode> projects =
          projectCaptures.stream().map(captures::get).collect(toImmutableList());

      return pullWindowNodeAboveProjects(captures.get(childCapture), projects)
          .flatMap(newChild -> manipulateAdjacentWindowNodes(parent, newChild, context))
          .map(Result::ofPlanNode)
          .orElse(Result.empty());
    }

    protected abstract Optional<PlanNode> manipulateAdjacentWindowNodes(
        WindowNode parent, WindowNode child, Context context);

    /**
     * Looks for the pattern (ProjectNode*)WindowNode, and rewrites it to WindowNode(ProjectNode*),
     * returning an empty option if it can't rewrite the projects, for example because they rely on
     * the output of the WindowNode.
     *
     * @param projects the nodes above the target, bottom first.
     */
    protected static Optional<WindowNode> pullWindowNodeAboveProjects(
        WindowNode target, List<ProjectNode> projects) {
      if (projects.isEmpty()) {
        return Optional.of(target);
      }

      PlanNode targetChild = target.getChild();

      Set<Symbol> targetInputs = ImmutableSet.copyOf(targetChild.getOutputSymbols());
      Set<Symbol> targetOutputs = ImmutableSet.copyOf(target.getOutputSymbols());

      PlanNode newTargetChild = targetChild;

      for (ProjectNode project : projects) {
        Set<Symbol> newTargetChildOutputs = ImmutableSet.copyOf(newTargetChild.getOutputSymbols());

        // The only kind of use of the output of the target that we can safely ignore is a simple
        // identity propagation.
        // The target node, when hoisted above the projections, will provide the symbols directly.
        Map<Symbol, Expression> assignmentsWithoutTargetOutputIdentities =
            Maps.filterKeys(
                project.getAssignments().getMap(),
                output ->
                    !(project.getAssignments().isIdentity(output)
                        && targetOutputs.contains(output)));

        if (targetInputs.stream().anyMatch(assignmentsWithoutTargetOutputIdentities::containsKey)) {
          // Redefinition of an input to the target -- can't handle this case.
          return Optional.empty();
        }

        Assignments newAssignments =
            Assignments.builder()
                .putAll(assignmentsWithoutTargetOutputIdentities)
                .putIdentities(targetInputs)
                .build();

        if (!newTargetChildOutputs.containsAll(
            SymbolsExtractor.extractUnique(newAssignments.getExpressions()))) {
          // Projection uses an output of the target -- can't move the target above this projection.
          return Optional.empty();
        }

        newTargetChild = new ProjectNode(project.getPlanNodeId(), newTargetChild, newAssignments);
      }

      WindowNode newTarget = (WindowNode) target.replaceChildren(ImmutableList.of(newTargetChild));
      Set<Symbol> newTargetOutputs = ImmutableSet.copyOf(newTarget.getOutputSymbols());
      if (!newTargetOutputs.containsAll(projects.get(projects.size() - 1).getOutputSymbols())) {
        // The new target node is hiding some of the projections, which makes this rewrite
        // incorrect.
        return Optional.empty();
      }
      return Optional.of(newTarget);
    }
  }

  public static class MergeAdjacentWindowsOverProjects
      extends ManipulateAdjacentWindowsOverProjects {
    public MergeAdjacentWindowsOverProjects(int numProjects) {
      super(numProjects);
    }

    @Override
    protected Optional<PlanNode> manipulateAdjacentWindowNodes(
        WindowNode parent, WindowNode child, Context context) {
      if (!child.getSpecification().equals(parent.getSpecification()) || dependsOn(parent, child)) {
        return Optional.empty();
      }

      ImmutableMap.Builder<Symbol, WindowNode.Function> functionsBuilder = ImmutableMap.builder();
      functionsBuilder.putAll(parent.getWindowFunctions());
      functionsBuilder.putAll(child.getWindowFunctions());

      WindowNode mergedWindowNode =
          new WindowNode(
              parent.getPlanNodeId(),
              child.getChild(),
              parent.getSpecification(),
              functionsBuilder.buildOrThrow(),
              parent.getHashSymbol(),
              parent.getPrePartitionedInputs(),
              parent.getPreSortedOrderPrefix());

      return Optional.of(
          restrictOutputs(
                  context.getIdAllocator(),
                  mergedWindowNode,
                  ImmutableSet.copyOf(parent.getOutputSymbols()))
              .orElse(mergedWindowNode));
    }
  }

  public static class SwapAdjacentWindowsBySpecifications
      extends ManipulateAdjacentWindowsOverProjects {
    public SwapAdjacentWindowsBySpecifications(int numProjects) {
      super(numProjects);
    }

    @Override
    protected Optional<PlanNode> manipulateAdjacentWindowNodes(
        WindowNode parent, WindowNode child, Context context) {
      if ((compare(parent, child) < 0) && (!dependsOn(parent, child))) {
        PlanNode transposedWindows = transpose(parent, child);
        return Optional.of(
            restrictOutputs(
                    context.getIdAllocator(),
                    transposedWindows,
                    ImmutableSet.copyOf(parent.getOutputSymbols()))
                .orElse(transposedWindows));
      }
      return Optional.empty();
    }

    private static int compare(WindowNode o1, WindowNode o2) {
      int comparison = comparePartitionBy(o1, o2);
      if (comparison != 0) {
        return comparison;
      }

      comparison = compareOrderBy(o1, o2);
      if (comparison != 0) {
        return comparison;
      }

      // If PartitionBy and OrderBy clauses are identical, let's establish an arbitrary order to
      // prevent non-deterministic results of swapping WindowNodes in such a case
      return o1.getPlanNodeId().toString().compareTo(o2.getPlanNodeId().toString());
    }

    private static int comparePartitionBy(WindowNode o1, WindowNode o2) {
      Iterator<Symbol> iterator1 = o1.getSpecification().getPartitionBy().iterator();
      Iterator<Symbol> iterator2 = o2.getSpecification().getPartitionBy().iterator();

      while (iterator1.hasNext() && iterator2.hasNext()) {
        Symbol symbol1 = iterator1.next();
        Symbol symbol2 = iterator2.next();

        int partitionByComparison = symbol1.compareTo(symbol2);
        if (partitionByComparison != 0) {
          return partitionByComparison;
        }
      }

      if (iterator1.hasNext()) {
        return 1;
      }
      if (iterator2.hasNext()) {
        return -1;
      }
      return 0;
    }

    private static int compareOrderBy(WindowNode o1, WindowNode o2) {
      if (!o1.getSpecification().getOrderingScheme().isPresent()
          && !o2.getSpecification().getOrderingScheme().isPresent()) {
        return 0;
      }
      if (o1.getSpecification().getOrderingScheme().isPresent()
          && !o2.getSpecification().getOrderingScheme().isPresent()) {
        return 1;
      }
      if (!o1.getSpecification().getOrderingScheme().isPresent()
          && o2.getSpecification().getOrderingScheme().isPresent()) {
        return -1;
      }

      OrderingScheme o1OrderingScheme = o1.getSpecification().getOrderingScheme().get();
      OrderingScheme o2OrderingScheme = o2.getSpecification().getOrderingScheme().get();
      Iterator<Symbol> iterator1 = o1OrderingScheme.getOrderBy().iterator();
      Iterator<Symbol> iterator2 = o2OrderingScheme.getOrderBy().iterator();

      while (iterator1.hasNext() && iterator2.hasNext()) {
        Symbol symbol1 = iterator1.next();
        Symbol symbol2 = iterator2.next();

        int orderByComparison = symbol1.compareTo(symbol2);
        if (orderByComparison != 0) {
          return orderByComparison;
        }
        int sortOrderComparison =
            o1OrderingScheme.getOrdering(symbol1).compareTo(o2OrderingScheme.getOrdering(symbol2));
        if (sortOrderComparison != 0) {
          return sortOrderComparison;
        }
      }

      if (iterator1.hasNext()) {
        return 1;
      }
      if (iterator2.hasNext()) {
        return -1;
      }
      return 0;
    }
  }

  private static boolean dependsOn(WindowNode parent, WindowNode child) {
    return parent.getSpecification().getPartitionBy().stream()
            .anyMatch(child.getCreatedSymbols()::contains)
        || (parent.getSpecification().getOrderingScheme().isPresent()
            && parent.getSpecification().getOrderingScheme().get().getOrderBy().stream()
                .anyMatch(child.getCreatedSymbols()::contains))
        || parent.getWindowFunctions().values().stream()
            .map(SymbolsExtractor::extractUnique)
            .flatMap(Collection::stream)
            .anyMatch(child.getCreatedSymbols()::contains);
  }
}
