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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Measure;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ClassifierValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.MatchNumberValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ScalarValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.groupingSets;

public class SymbolMapper {
  private final Function<Symbol, Symbol> mappingFunction;

  public SymbolMapper(Function<Symbol, Symbol> mappingFunction) {
    this.mappingFunction = requireNonNull(mappingFunction, "mappingFunction is null");
  }

  public static SymbolMapper symbolMapper(Map<Symbol, Symbol> mapping) {
    return new SymbolMapper(
        symbol -> {
          while (mapping.containsKey(symbol) && !mapping.get(symbol).equals(symbol)) {
            symbol = mapping.get(symbol);
          }
          return symbol;
        });
  }

  public static SymbolMapper symbolReallocator(
      Map<Symbol, Symbol> mapping, SymbolAllocator symbolAllocator) {
    return new SymbolMapper(
        symbol -> {
          if (mapping.containsKey(symbol)) {
            while (mapping.containsKey(symbol) && !mapping.get(symbol).equals(symbol)) {
              symbol = mapping.get(symbol);
            }
            // do not remap the symbol further
            mapping.put(symbol, symbol);
            return symbol;
          }
          Symbol newSymbol = symbolAllocator.newSymbol(symbol);
          mapping.put(symbol, newSymbol);
          // do not remap the symbol further
          mapping.put(newSymbol, newSymbol);
          return newSymbol;
        });
  }

  // Return the canonical mapping for the symbol.
  public Symbol map(Symbol symbol) {
    return mappingFunction.apply(symbol);
  }

  public ApplyNode.SetExpression map(ApplyNode.SetExpression expression) {
    if (expression instanceof ApplyNode.Exists) {
      return expression;
    } else if (expression instanceof ApplyNode.In) {
      ApplyNode.In in = (ApplyNode.In) expression;
      return new ApplyNode.In(map(in.getValue()), map(in.getReference()));
    } else if (expression instanceof ApplyNode.QuantifiedComparison) {
      ApplyNode.QuantifiedComparison comparison = (ApplyNode.QuantifiedComparison) expression;
      return new ApplyNode.QuantifiedComparison(
          comparison.getOperator(),
          comparison.getQuantifier(),
          map(comparison.getValue()),
          map(comparison.getReference()));
    } else {
      throw new IllegalArgumentException("Unexpected value: " + expression);
    }
  }

  public List<Symbol> map(List<Symbol> symbols) {
    return symbols.stream().map(this::map).collect(toImmutableList());
  }

  public List<Symbol> mapAndDistinct(List<Symbol> symbols) {
    return symbols.stream().map(this::map).distinct().collect(toImmutableList());
  }

  public DataOrganizationSpecification mapAndDistinct(DataOrganizationSpecification specification) {
    return new DataOrganizationSpecification(
        mapAndDistinct(specification.getPartitionBy()),
        specification.getOrderingScheme().map(this::map));
  }

  public Expression map(Expression expression) {
    return ExpressionTreeRewriter.rewriteWith(
        new ExpressionRewriter<Void>() {
          @Override
          public Expression rewriteSymbolReference(
              SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Symbol canonical = map(Symbol.from(node));
            return canonical.toSymbolReference();
          }
        },
        expression);
  }

  public AggregationNode map(AggregationNode node, PlanNode source) {
    return map(node, source, node.getPlanNodeId());
  }

  public AggregationNode map(AggregationNode node, PlanNode source, PlanNodeId newNodeId) {
    ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
    for (Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      aggregations.put(map(entry.getKey()), map(entry.getValue()));
    }

    return new AggregationNode(
        newNodeId,
        source,
        aggregations.buildOrThrow(),
        groupingSets(
            mapAndDistinct(node.getGroupingKeys()),
            node.getGroupingSetCount(),
            node.getGlobalGroupingSets()),
        ImmutableList.of(),
        node.getStep(),
        node.getHashSymbol().map(this::map),
        node.getGroupIdSymbol().map(this::map));
  }

  public AggregationNode.Aggregation map(AggregationNode.Aggregation aggregation) {
    return new AggregationNode.Aggregation(
        aggregation.getResolvedFunction(),
        aggregation.getArguments().stream().map(this::map).collect(toImmutableList()),
        aggregation.isDistinct(),
        aggregation.getFilter().map(this::map),
        aggregation.getOrderingScheme().map(this::map),
        aggregation.getMask().map(this::map));
  }

  /*private ExpressionAndValuePointers map(ExpressionAndValuePointers expressionAndValuePointers)
  {
      // Map only the input symbols of ValuePointers. These are the symbols produced by the source node.
      // Other symbols present in the ExpressionAndValuePointers structure are synthetic unique symbols
      // with no outer usage or dependencies.
      ImmutableList.Builder<ExpressionAndValuePointers.Assignment> newAssignments = ImmutableList.builder();
      for (ExpressionAndValuePointers.Assignment assignment : expressionAndValuePointers.getAssignments()) {
          ValuePointer newPointer = switch (assignment.valuePointer()) {
              case ClassifierValuePointer pointer -> pointer;
              case MatchNumberValuePointer pointer -> pointer;
              case ScalarValuePointer pointer -> new ScalarValuePointer(pointer.getLogicalIndexPointer(), map(pointer.getInputSymbol()));
              case AggregationValuePointer pointer -> {
                  List<Expression> newArguments = pointer.getArguments().stream()
                          .map(expression -> ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
                          {
                              @Override
                              public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                              {
                                  if (pointer.getClassifierSymbol().isPresent() && Symbol.from(node).equals(pointer.getClassifierSymbol().get()) ||
                                          pointer.getMatchNumberSymbol().isPresent() && Symbol.from(node).equals(pointer.getMatchNumberSymbol().get())) {
                                      return node;
                                  }
                                  return map(node);
                              }
                          }, expression))
                          .collect(toImmutableList());

                  yield new AggregationValuePointer(
                          pointer.getFunction(),
                          pointer.getSetDescriptor(),
                          newArguments,
                          pointer.getClassifierSymbol(),
                          pointer.getMatchNumberSymbol());
              }
          };

          newAssignments.add(new ExpressionAndValuePointers.Assignment(assignment.symbol(), newPointer));
      }

      return new ExpressionAndValuePointers(expressionAndValuePointers.getExpression(), newAssignments.build());
  }*/

  public LimitNode map(LimitNode node, PlanNode source) {
    return new LimitNode(
        node.getPlanNodeId(),
        source,
        node.getCount(),
        node.getTiesResolvingScheme().map(this::map));
  }

  public OrderingScheme map(OrderingScheme orderingScheme) {
    ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
    ImmutableMap.Builder<Symbol, SortOrder> newOrderings = ImmutableMap.builder();
    Set<Symbol> added = new HashSet<>(orderingScheme.getOrderBy().size());
    for (Symbol symbol : orderingScheme.getOrderBy()) {
      Symbol canonical = map(symbol);
      if (added.add(canonical)) {
        newSymbols.add(canonical);
        newOrderings.put(canonical, orderingScheme.getOrdering(symbol));
      }
    }
    return new OrderingScheme(newSymbols.build(), newOrderings.buildOrThrow());
  }

  public WindowNode map(WindowNode node, PlanNode source) {
    ImmutableMap.Builder<Symbol, WindowNode.Function> newFunctions = ImmutableMap.builder();
    node.getWindowFunctions()
        .forEach(
            (symbol, function) -> {
              List<Expression> newArguments =
                  function.getArguments().stream().map(this::map).collect(toImmutableList());
              WindowNode.Frame newFrame = map(function.getFrame());

              newFunctions.put(
                  map(symbol),
                  new WindowNode.Function(
                      function.getResolvedFunction(),
                      newArguments,
                      newFrame,
                      function.isIgnoreNulls()));
            });

    ImmutableList<Symbol> newPartitionBy =
        node.getSpecification().getPartitionBy().stream().map(this::map).collect(toImmutableList());
    Optional<OrderingScheme> newOrderingScheme =
        node.getSpecification().getOrderingScheme().map(this::map);
    DataOrganizationSpecification newSpecification =
        new DataOrganizationSpecification(newPartitionBy, newOrderingScheme);

    return new WindowNode(
        node.getPlanNodeId(),
        source,
        newSpecification,
        newFunctions.buildOrThrow(),
        node.getHashSymbol().map(this::map),
        node.getPrePartitionedInputs().stream().map(this::map).collect(toImmutableSet()),
        node.getPreSortedOrderPrefix());
  }

  private WindowNode.Frame map(WindowNode.Frame frame) {
    return new WindowNode.Frame(
        frame.getType(),
        frame.getStartType(),
        frame.getStartValue().map(this::map),
        frame.getSortKeyCoercedForFrameStartComparison().map(this::map),
        frame.getEndType(),
        frame.getEndValue().map(this::map),
        frame.getSortKeyCoercedForFrameEndComparison().map(this::map),
        frame.getOriginalStartValue(),
        frame.getOriginalEndValue());
  }

  public TopKNode map(TopKNode node, List<PlanNode> source) {
    return map(node, source, node.getPlanNodeId());
  }

  public TopKNode map(TopKNode node, List<PlanNode> source, PlanNodeId nodeId) {
    return new TopKNode(
        nodeId,
        source,
        map(node.getOrderingScheme()),
        node.getCount(),
        node.getOutputSymbols().stream().map(this::map).collect(Collectors.toList()),
        node.isChildrenDataInOrder());
  }

  public PatternRecognitionNode map(PatternRecognitionNode node, PlanNode source) {
    ImmutableMap.Builder<Symbol, Measure> newMeasures = ImmutableMap.builder();
    node.getMeasures()
        .forEach(
            (symbol, measure) -> {
              ExpressionAndValuePointers newExpression =
                  map(measure.getExpressionAndValuePointers());
              newMeasures.put(map(symbol), new Measure(newExpression, measure.getType()));
            });

    ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> newVariableDefinitions =
        ImmutableMap.builder();
    node.getVariableDefinitions()
        .forEach((label, expression) -> newVariableDefinitions.put(label, map(expression)));

    return new PatternRecognitionNode(
        node.getPlanNodeId(),
        source,
        mapAndDistinct(node.getPartitionBy()),
        node.getOrderingScheme(),
        node.getHashSymbol().map(this::map),
        newMeasures.buildOrThrow(),
        node.getRowsPerMatch(),
        node.getSkipToLabels(),
        node.getSkipToPosition(),
        node.getPattern(),
        newVariableDefinitions.buildOrThrow());
  }

  private ExpressionAndValuePointers map(ExpressionAndValuePointers expressionAndValuePointers) {
    // Map only the input symbols of ValuePointers. These are the symbols produced by the source
    // node.
    // Other symbols present in the ExpressionAndValuePointers structure are synthetic unique
    // symbols
    // with no outer usage or dependencies.
    ImmutableList.Builder<ExpressionAndValuePointers.Assignment> newAssignments =
        ImmutableList.builder();
    for (ExpressionAndValuePointers.Assignment assignment :
        expressionAndValuePointers.getAssignments()) {
      ValuePointer newPointer;
      if (assignment.getValuePointer() instanceof ClassifierValuePointer) {
        newPointer = (ClassifierValuePointer) assignment.getValuePointer();
      } else if (assignment.getValuePointer() instanceof MatchNumberValuePointer) {
        newPointer = (MatchNumberValuePointer) assignment.getValuePointer();
      } else if (assignment.getValuePointer() instanceof ScalarValuePointer) {
        ScalarValuePointer pointer = (ScalarValuePointer) assignment.getValuePointer();
        newPointer =
            new ScalarValuePointer(pointer.getLogicalIndexPointer(), map(pointer.getInputSymbol()));
      } else {
        throw new IllegalArgumentException(
            "Unsupported ValuePointer type: " + assignment.getValuePointer().getClass().getName());
      }

      newAssignments.add(
          new ExpressionAndValuePointers.Assignment(assignment.getSymbol(), newPointer));
    }

    return new ExpressionAndValuePointers(
        expressionAndValuePointers.getExpression(), newAssignments.build());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();

    public void put(Symbol from, Symbol to) {
      mappings.put(from, to);
    }

    public SymbolMapper build() {
      return SymbolMapper.symbolMapper(mappings.buildOrThrow());
    }
  }
}
