/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ExpressionSymbolInliner.inlineSymbols;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.isEffectivelyLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * Inlines expressions from a child project node into a parent project node as long as they are
 * simple constants, or they are referenced only once (to avoid introducing duplicate computation)
 * and the references don't appear within a TRY block (to avoid changing semantics).
 */
public class InlineProjections implements Rule<ProjectNode> {
  private static final Capture<ProjectNode> CHILD = newCapture();

  private static final Pattern<ProjectNode> PATTERN =
      project().with(source().matching(project().capturedAs(CHILD)));

  private final PlannerContext plannerContext;

  public InlineProjections(PlannerContext plannerContext) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
  }

  @Override
  public Pattern<ProjectNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(ProjectNode parent, Captures captures, Context context) {
    ProjectNode child = captures.get(CHILD);

    return inlineProjections(
            plannerContext,
            parent,
            child,
            context.getSessionInfo(),
            context.getSymbolAllocator().getTypes())
        .map(Result::ofPlanNode)
        .orElse(Result.empty());
  }

  static Optional<ProjectNode> inlineProjections(
      PlannerContext plannerContext,
      ProjectNode parent,
      ProjectNode child,
      SessionInfo sessionInfo,
      TypeProvider types) {
    // squash identity projections
    if (parent.isIdentity() && child.isIdentity()) {
      return Optional.of((ProjectNode) parent.replaceChildren(ImmutableList.of(child.getChild())));
    }

    Set<Symbol> targets = extractInliningTargets(plannerContext, parent, child, sessionInfo, types);
    if (targets.isEmpty()) {
      return Optional.empty();
    }

    // inline the expressions
    Assignments assignments = child.getAssignments().filter(targets::contains);
    Map<Symbol, Expression> parentAssignments =
        parent.getAssignments().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> inlineReferences(entry.getValue(), assignments)));

    // Synthesize identity assignments for the inputs of expressions that were inlined
    // to place in the child projection.
    Set<Symbol> inputs =
        child.getAssignments().entrySet().stream()
            .filter(entry -> targets.contains(entry.getKey()))
            .map(Map.Entry::getValue)
            .flatMap(entry -> SymbolsExtractor.extractAll(entry).stream())
            .collect(toSet());

    Assignments.Builder newChildAssignmentsBuilder = Assignments.builder();
    for (Map.Entry<Symbol, Expression> assignment : child.getAssignments().entrySet()) {
      if (!targets.contains(assignment.getKey())) {
        newChildAssignmentsBuilder.put(assignment);
        // If this is not an identity assignment, remove the symbol from inputs, as we don't want to
        // reset the expression
        if (!isSymbolReference(assignment.getKey(), assignment.getValue())) {
          inputs.remove(assignment.getKey());
        }
      }
    }
    for (Symbol input : inputs) {
      newChildAssignmentsBuilder.putIdentity(input);
    }

    Assignments newChildAssignments = newChildAssignmentsBuilder.build();
    PlanNode newChild;
    if (newChildAssignments.isIdentity()) {
      newChild = child.getChild();
    } else {
      newChild = new ProjectNode(child.getPlanNodeId(), child.getChild(), newChildAssignments);
    }

    return Optional.of(
        new ProjectNode(parent.getPlanNodeId(), newChild, Assignments.copyOf(parentAssignments)));
  }

  private static Expression inlineReferences(Expression expression, Assignments assignments) {
    Function<Symbol, Expression> mapping =
        symbol -> {
          Expression result = assignments.get(symbol);
          if (result != null) {
            return result;
          }

          return symbol.toSymbolReference();
        };

    return inlineSymbols(mapping, expression);
  }

  private static Set<Symbol> extractInliningTargets(
      PlannerContext plannerContext,
      ProjectNode parent,
      ProjectNode child,
      SessionInfo sessionInfo,
      TypeProvider types) {
    // candidates for inlining are
    //   1. references to simple constants or symbol references
    //   2. references to complex expressions that
    //      a. are not inputs to try() expressions
    //      b. appear only once across all expressions
    //      c. are not identity projections
    // which come from the child, as opposed to an enclosing scope.

    Set<Symbol> childOutputSet = ImmutableSet.copyOf(child.getOutputSymbols());

    Map<Symbol, Long> dependencies =
        parent.getAssignments().getExpressions().stream()
            .flatMap(expression -> SymbolsExtractor.extractAll(expression).stream())
            .filter(childOutputSet::contains)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    // find references to simple constants or symbol references
    Set<Symbol> basicReferences =
        dependencies.keySet().stream()
            .filter(
                input ->
                    isEffectivelyLiteral(
                            child.getAssignments().get(input), plannerContext, sessionInfo)
                        || child.getAssignments().get(input) instanceof SymbolReference)
            .filter(
                input ->
                    !child
                        .getAssignments()
                        .isIdentity(
                            input)) // skip identities, otherwise, this rule will keep firing
            // forever
            .collect(toSet());

    Set<Symbol> singletons =
        dependencies.entrySet().stream()
            .filter(
                entry ->
                    entry.getValue()
                        == 1) // reference appears just once across all expressions in parent
            // project node
            .filter(
                entry ->
                    !child
                        .getAssignments()
                        .isIdentity(
                            entry.getKey())) // skip identities, otherwise, this rule will keep
            .map(Map.Entry::getKey)
            .collect(toSet());

    return Sets.union(singletons, basicReferences);
  }

  private static boolean isSymbolReference(Symbol symbol, Expression expression) {
    return expression instanceof SymbolReference
        && ((SymbolReference) expression).getName().equals(symbol.getName());
  }
}
