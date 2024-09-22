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
package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
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

  /*public ApplyNode.SetExpression map(ApplyNode.SetExpression expression)
  {
      return switch (expression) {
          case ApplyNode.Exists exists -> exists;
          case ApplyNode.In in -> new ApplyNode.In(map(in.value()), map(in.reference()));
          case ApplyNode.QuantifiedComparison comparison -> new ApplyNode.QuantifiedComparison(
                  comparison.operator(),
                  comparison.quantifier(),
                  map(comparison.value()),
                  map(comparison.reference()));
      };
  }*/

  public List<Symbol> map(List<Symbol> symbols) {
    return symbols.stream().map(this::map).collect(toImmutableList());
  }

  public List<Symbol> mapAndDistinct(List<Symbol> symbols) {
    return symbols.stream().map(this::map).distinct().collect(toImmutableList());
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
