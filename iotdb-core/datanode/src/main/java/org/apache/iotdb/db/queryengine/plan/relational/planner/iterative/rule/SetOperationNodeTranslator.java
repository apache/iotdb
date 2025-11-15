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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.FunctionNullability;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SetOperationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FrameBound;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowFrame;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tsfile.read.common.type.Type;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.Util.getResolvedBuiltInAggregateFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toSqlType;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.LongType.INT64;

public class SetOperationNodeTranslator {

  private static final String MARKER = "marker";
  private static final String COUNT_MARKER = "count";
  private static final String ROW_NUMBER_SYMBOL = "row_number";
  private final SymbolAllocator symbolAllocator;
  private final QueryId idAllocator;
  private final Metadata metadata;

  public SetOperationNodeTranslator(
      Metadata metadata, SymbolAllocator symbolAllocator, QueryId idAllocator) {

    this.metadata = requireNonNull(metadata, "metadata is null");
    this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
    this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
  }

  /** for intersect distinct and except distinct , use true and false for markers */
  public TranslationResult makeSetContainmentPlanForDistinct(SetOperationNode node) {

    checkArgument(!(node instanceof UnionNode), "Cannot simplify a UnionNode");
    List<Symbol> markers = allocateSymbols(node.getChildren().size(), MARKER, BOOLEAN);

    // 1. add the marker column to the origin planNode
    List<PlanNode> projectNodesWithMarkers = appendMarkers(markers, node.getChildren(), node);

    // 2. add the union node over all new projection nodes.
    // The outputs of the union must have the same name as the original intersect node
    UnionNode union =
        union(
            projectNodesWithMarkers,
            ImmutableList.copyOf(concat(node.getOutputSymbols(), markers)));

    // 3. add the aggregation node above the union node
    List<Symbol> aggregationOutputs = allocateSymbols(markers.size(), COUNT, INT64);
    AggregationNode aggregation =
        computeCounts(union, node.getOutputSymbols(), markers, aggregationOutputs);

    return new TranslationResult(aggregation, aggregationOutputs);
  }

  /** for intersect all and except all, use true and false for markers */
  public TranslationResult makeSetContainmentPlanForAll(SetOperationNode node) {

    checkArgument(!(node instanceof UnionNode), "Cannot simplify a UnionNode");
    List<Symbol> markers = allocateSymbols(node.getChildren().size(), MARKER, BOOLEAN);

    // for every child of SetOperation node, add the marker column for the child
    List<PlanNode> projectNodesWithMarkers = appendMarkers(markers, node.getChildren(), node);

    UnionNode union =
        union(
            projectNodesWithMarkers,
            ImmutableList.copyOf(concat(node.getOutputSymbols(), markers)));
    List<Symbol> countOutputs = allocateSymbols(markers.size(), COUNT_MARKER, INT64);
    Symbol rowNumberSymbol = symbolAllocator.newSymbol(ROW_NUMBER_SYMBOL, INT64);
    WindowNode windowNode =
        appendCounts(union, node.getOutputSymbols(), markers, countOutputs, rowNumberSymbol);

    ProjectNode projectNode =
        new ProjectNode(
            idAllocator.genPlanNodeId(),
            windowNode,
            Assignments.identity(
                ImmutableList.copyOf(
                    concat(
                        node.getOutputSymbols(),
                        countOutputs,
                        ImmutableList.of(rowNumberSymbol)))));

    return new TranslationResult(projectNode, countOutputs, Optional.of(rowNumberSymbol));
  }

  /**
   * for transforming the intersectNode (all) and exceptNode(all), add the window node and group
   * node above the union node
   */
  private WindowNode appendCounts(
      UnionNode union,
      List<Symbol> originOutputSymbols,
      List<Symbol> markers,
      List<Symbol> countOutputs,
      Symbol rowNumberSymbol) {

    checkArgument(
        markers.size() == countOutputs.size(),
        "the size of markers should be same as the size of count output symbols");

    // Add group node above the union node to assist partitioning, preparing for the window node
    ImmutableMap.Builder<Symbol, SortOrder> sortOrderings = ImmutableMap.builder();
    ImmutableList.Builder<Symbol> sortSymbolBuilder = ImmutableList.builder();
    for (Symbol originalColumn : originOutputSymbols) {
      sortSymbolBuilder.add(originalColumn);
      sortOrderings.put(originalColumn, SortOrder.ASC_NULLS_LAST);
    }
    ImmutableList<Symbol> sortSymbols = sortSymbolBuilder.build();
    GroupNode groupNode =
        new GroupNode(
            idAllocator.genPlanNodeId(),
            union,
            new OrderingScheme(sortSymbols, sortOrderings.build()),
            sortSymbols.size());

    // build the windowFunctions for count(marker) and row_number
    ImmutableMap.Builder<Symbol, WindowNode.Function> windowFunctions = ImmutableMap.builder();
    WindowNode.Frame windowFunctionFrame =
        new WindowNode.Frame(
            WindowFrame.Type.ROWS,
            FrameBound.Type.UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            FrameBound.Type.UNBOUNDED_FOLLOWING,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    ResolvedFunction countFunction =
        getResolvedBuiltInAggregateFunction(
            metadata, SqlConstant.COUNT, Collections.singletonList(BOOLEAN));
    for (int i = 0; i < markers.size(); i++) {
      windowFunctions.put(
          countOutputs.get(i),
          new WindowNode.Function(
              countFunction,
              ImmutableList.of(markers.get(i).toSymbolReference()),
              windowFunctionFrame,
              false));
    }

    List<Type> argumentTypes = ImmutableList.of();
    ResolvedFunction rowNumberFunction =
        new ResolvedFunction(
            new BoundSignature(
                SqlConstant.ROW_NUMBER,
                metadata.getFunctionReturnType(SqlConstant.ROW_NUMBER, argumentTypes),
                argumentTypes),
            FunctionId.NOOP_FUNCTION_ID,
            FunctionKind.WINDOW,
            true,
            FunctionNullability.getAggregationFunctionNullability(argumentTypes.size()));

    windowFunctions.put(
        rowNumberSymbol,
        new WindowNode.Function(rowNumberFunction, ImmutableList.of(), windowFunctionFrame, false));

    // add the windowNode above the group node
    return new WindowNode(
        idAllocator.genPlanNodeId(),
        groupNode,
        new DataOrganizationSpecification(originOutputSymbols, Optional.empty()),
        windowFunctions.buildOrThrow(),
        Optional.empty(),
        ImmutableSet.of(),
        0);
  }

  /** get an array of markers, used for the new columns */
  private List<Symbol> allocateSymbols(int count, String nameHint, Type type) {
    ImmutableList.Builder<Symbol> symbolsBuilder = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      symbolsBuilder.add(symbolAllocator.newSymbol(nameHint, type));
    }
    return symbolsBuilder.build();
  }

  /**
   * Builds projection nodes with marker columns for each child of the set operation. Each child
   * gets TRUE for its own marker and NULL (cast to BOOLEAN) for others. This is used in the
   * implementation of INTERSECT and EXCEPT set operations.
   */
  private List<PlanNode> appendMarkers(
      List<Symbol> markers, List<PlanNode> children, SetOperationNode node) {
    ImmutableList.Builder<PlanNode> projectionsWithMarker = ImmutableList.builder();

    Map<Symbol, Collection<Symbol>> symbolMapping = node.getSymbolMapping().asMap();
    for (int childIndex = 0; childIndex < children.size(); childIndex++) {

      // add the original symbols to projection node
      Assignments.Builder assignments = Assignments.builder();
      for (Symbol outputSymbol : node.getOutputSymbols()) {
        Collection<Symbol> inputSymbols = symbolMapping.get(outputSymbol);
        Symbol sourceSymbol = Iterables.get(inputSymbols, childIndex);

        Symbol newProjectedSymbol = symbolAllocator.newSymbol(outputSymbol);
        assignments.put(newProjectedSymbol, sourceSymbol.toSymbolReference());
      }

      // add the new marker symbol to the new projection node
      for (int j = 0; j < markers.size(); j++) {
        Expression expression =
            j == childIndex ? TRUE_LITERAL : new Cast(new NullLiteral(), toSqlType(BOOLEAN));
        assignments.put(symbolAllocator.newSymbol(markers.get(j).getName(), BOOLEAN), expression);
      }

      projectionsWithMarker.add(
          new ProjectNode(
              idAllocator.genPlanNodeId(), children.get(childIndex), assignments.build()));
    }

    return projectionsWithMarker.build();
  }

  private UnionNode union(List<PlanNode> projectNodesWithMarkers, List<Symbol> outputs) {

    ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputs = ImmutableListMultimap.builder();

    for (PlanNode projectionNode : projectNodesWithMarkers) {
      List<Symbol> outputSymbols = projectionNode.getOutputSymbols();
      for (int i = 0; i < outputSymbols.size(); i++) {
        outputsToInputs.put(outputs.get(i), outputSymbols.get(i));
      }
    }

    return new UnionNode(
        idAllocator.genPlanNodeId(), projectNodesWithMarkers, outputsToInputs.build(), outputs);
  }

  /** add the aggregation node above the union node */
  private AggregationNode computeCounts(
      UnionNode unionNode,
      List<Symbol> originalColumns,
      List<Symbol> markers,
      List<Symbol> aggregationOutputs) {

    ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();

    ResolvedFunction resolvedFunction =
        getResolvedBuiltInAggregateFunction(metadata, COUNT, Collections.singletonList(BOOLEAN));

    for (int i = 0; i < markers.size(); i++) {
      Symbol countMarker = aggregationOutputs.get(i);
      aggregations.put(
          countMarker,
          new AggregationNode.Aggregation(
              resolvedFunction,
              ImmutableList.of(markers.get(i).toSymbolReference()),
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty()));
    }

    return AggregationNode.singleAggregation(
        idAllocator.genPlanNodeId(),
        unionNode,
        aggregations.buildOrThrow(),
        singleGroupingSet(originalColumns));
  }

  public static class TranslationResult {

    private final PlanNode planNode;
    private final List<Symbol> countSymbols;
    private final Optional<Symbol> rowNumberSymbol;

    public TranslationResult(PlanNode planNode, List<Symbol> countSymbols) {
      this(planNode, countSymbols, Optional.empty());
    }

    public TranslationResult(
        PlanNode planNode, List<Symbol> countSymbols, Optional<Symbol> rowNumberSymbol) {
      this.planNode = requireNonNull(planNode, "planNode is null");
      this.countSymbols =
          ImmutableList.copyOf(requireNonNull(countSymbols, "countSymbols is null"));
      this.rowNumberSymbol = requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
    }

    public List<Symbol> getCountSymbols() {
      return countSymbols;
    }

    public Symbol getRowNumberSymbol() {
      checkState(rowNumberSymbol.isPresent(), "rowNumberSymbol is empty");
      return rowNumberSymbol.get();
    }

    public PlanNode getPlanNode() {
      return this.planNode;
    }
  }
}
