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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.NodeAndMappings;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SymbolMapper.symbolMapper;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SymbolMapper.symbolReallocator;

/**
 * Re-maps symbol references that are just aliases of each other (e.g., due to projections like
 * {@code $0 := $1})
 *
 * <p>E.g.,
 *
 * <p>{@code Output[$0, $1] -> Project[$0 := $2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] ->
 * ...}
 *
 * <p>gets rewritten as
 *
 * <p>{@code Output[$2, $1] -> Project[$2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 */
public class UnaliasSymbolReferences implements PlanOptimizer {
  private final Metadata metadata;

  public UnaliasSymbolReferences(Metadata metadata) {
    this.metadata = requireNonNull(metadata, "metadata is null");
  }

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    requireNonNull(plan, "plan is null");

    Visitor visitor = new Visitor(metadata, SymbolMapper::symbolMapper);
    PlanAndMappings result = plan.accept(visitor, UnaliasContext.empty());
    return result.getRoot();
  }

  /**
   * Replace all symbols in the plan with new symbols. The returned plan has different output than
   * the original plan. Also, the order of symbols might change during symbol replacement. Symbols
   * in the list `fields` are replaced maintaining the order so they might be used to match original
   * symbols with their replacements. Replacing symbols helps avoid collisions when symbols or parts
   * of the plan are reused.
   */
  public NodeAndMappings reallocateSymbols(
      PlanNode plan, List<Symbol> fields, SymbolAllocator symbolAllocator) {
    requireNonNull(plan, "plan is null");
    requireNonNull(fields, "fields is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");

    Visitor visitor = new Visitor(metadata, mapping -> symbolReallocator(mapping, symbolAllocator));
    PlanAndMappings result = plan.accept(visitor, UnaliasContext.empty());
    return new NodeAndMappings(result.getRoot(), symbolMapper(result.getMappings()).map(fields));
  }

  private static class Visitor extends PlanVisitor<PlanAndMappings, UnaliasContext> {
    private final Metadata metadata;
    private final Function<Map<Symbol, Symbol>, SymbolMapper> mapperProvider;

    public Visitor(Metadata metadata, Function<Map<Symbol, Symbol>, SymbolMapper> mapperProvider) {
      this.metadata = requireNonNull(metadata, "metadata is null");
      this.mapperProvider = requireNonNull(mapperProvider, "mapperProvider is null");
    }

    private SymbolMapper symbolMapper(Map<Symbol, Symbol> mappings) {
      return mapperProvider.apply(mappings);
    }

    @Override
    public PlanAndMappings visitPlan(PlanNode node, UnaliasContext context) {
      throw new UnsupportedOperationException(
          "Unsupported plan node " + node.getClass().getSimpleName());
    }

    @Override
    public PlanAndMappings visitAggregation(AggregationNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);
      Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
      SymbolMapper mapper = symbolMapper(mapping);

      AggregationNode rewrittenAggregation = mapper.map(node, rewrittenSource.getRoot());

      return new PlanAndMappings(rewrittenAggregation, mapping);
    }

    @Override
    public PlanAndMappings visitDeviceTableScan(DeviceTableScanNode node, UnaliasContext context) {
      Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
      SymbolMapper mapper = symbolMapper(mapping);

      List<Symbol> newOutputs = mapper.map(node.getOutputSymbols());

      Map<Symbol, ColumnSchema> newAssignments = new HashMap<>();
      node.getAssignments()
          .forEach(
              (symbol, handle) -> {
                Symbol newSymbol = mapper.map(symbol);
                newAssignments.put(newSymbol, handle);
              });

      return new PlanAndMappings(
          new DeviceTableScanNode(
              node.getPlanNodeId(),
              node.getQualifiedObjectName(),
              newOutputs,
              newAssignments,
              node.getDeviceEntries(),
              node.getIdAndAttributeIndexMap(),
              node.getScanOrder(),
              node.getTimePredicate().orElse(null),
              node.getPushDownPredicate(),
              node.getPushDownLimit(),
              node.getPushDownOffset(),
              node.isPushLimitToEachDevice()),
          mapping);
    }

    @Override
    public PlanAndMappings visitInformationSchemaTableScan(
        InformationSchemaTableScanNode node, UnaliasContext context) {
      Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
      SymbolMapper mapper = symbolMapper(mapping);

      List<Symbol> newOutputs = mapper.map(node.getOutputSymbols());

      Map<Symbol, ColumnSchema> newAssignments = new HashMap<>();
      node.getAssignments()
          .forEach(
              (symbol, handle) -> {
                Symbol newSymbol = mapper.map(symbol);
                newAssignments.put(newSymbol, handle);
              });

      return new PlanAndMappings(
          new InformationSchemaTableScanNode(
              node.getPlanNodeId(),
              node.getQualifiedObjectName(),
              newOutputs,
              newAssignments,
              node.getPushDownPredicate(),
              node.getPushDownLimit(),
              node.getPushDownOffset()),
          mapping);
    }

    @Override
    public PlanAndMappings visitGapFill(GapFillNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);
      Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
      SymbolMapper mapper = symbolMapper(mapping);

      List<Symbol> groupingKeys = Collections.emptyList();
      if (!node.getGapFillGroupingKeys().isEmpty()) {
        groupingKeys = mapper.mapAndDistinct(node.getGapFillGroupingKeys());
      }

      return new PlanAndMappings(
          new GapFillNode(
              node.getPlanNodeId(),
              rewrittenSource.getRoot(),
              node.getStartTime(),
              node.getEndTime(),
              node.getMonthDuration(),
              node.getNonMonthDuration(),
              mapper.map(node.getGapFillColumn()),
              groupingKeys),
          mapping);
    }

    @Override
    public PlanAndMappings visitPreviousFill(PreviousFillNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);

      if (node.getHelperColumn().isPresent() || node.getGroupingKeys().isPresent()) {
        Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
        SymbolMapper mapper = symbolMapper(mapping);

        Symbol helperColumn = null;
        if (node.getHelperColumn().isPresent()) {
          helperColumn = mapper.map(node.getHelperColumn().get());
        }
        List<Symbol> groupingKeys = null;
        if (node.getGroupingKeys().isPresent()) {
          groupingKeys = mapper.mapAndDistinct(node.getGroupingKeys().get());
        }
        return new PlanAndMappings(
            new PreviousFillNode(
                node.getPlanNodeId(),
                rewrittenSource.getRoot(),
                node.getTimeBound().orElse(null),
                helperColumn,
                groupingKeys),
            mapping);
      } else {
        return new PlanAndMappings(
            node.replaceChildren(ImmutableList.of(rewrittenSource.getRoot())),
            rewrittenSource.getMappings());
      }
    }

    @Override
    public PlanAndMappings visitLinearFill(LinearFillNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);
      Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
      SymbolMapper mapper = symbolMapper(mapping);

      List<Symbol> groupingKeys = null;
      if (node.getGroupingKeys().isPresent()) {
        groupingKeys = mapper.mapAndDistinct(node.getGroupingKeys().get());
      }

      return new PlanAndMappings(
          new LinearFillNode(
              node.getPlanNodeId(),
              rewrittenSource.getRoot(),
              mapper.map(node.getHelperColumn()),
              groupingKeys),
          mapping);
    }

    @Override
    public PlanAndMappings visitValueFill(ValueFillNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);

      return new PlanAndMappings(
          node.replaceChildren(ImmutableList.of(rewrittenSource.getRoot())),
          rewrittenSource.getMappings());
    }

    @Override
    public PlanAndMappings visitOffset(OffsetNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);

      return new PlanAndMappings(
          node.replaceChildren(ImmutableList.of(rewrittenSource.getRoot())),
          rewrittenSource.getMappings());
    }

    @Override
    public PlanAndMappings visitExplainAnalyze(ExplainAnalyzeNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);

      return new PlanAndMappings(
          node.replaceChildren(ImmutableList.of(rewrittenSource.getRoot())),
          rewrittenSource.getMappings());
    }

    @Override
    public PlanAndMappings visitLimit(LimitNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);
      Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
      SymbolMapper mapper = symbolMapper(mapping);

      LimitNode rewrittenLimit = mapper.map(node, rewrittenSource.getRoot());

      return new PlanAndMappings(rewrittenLimit, mapping);
    }

    @Override
    public PlanAndMappings visitTopK(TopKNode node, UnaliasContext context) {
      List<PlanAndMappings> rewrittenSources =
          node.getChildren().stream()
              .map(child -> child.accept(this, context))
              .collect(Collectors.toList());
      Map<Symbol, Symbol> mapping = new HashMap<>();
      rewrittenSources.forEach(map -> map.getMappings().forEach(mapping::putIfAbsent));
      SymbolMapper mapper = symbolMapper(mapping);

      TopKNode rewrittenTopN =
          mapper.map(
              node,
              rewrittenSources.stream().map(PlanAndMappings::getRoot).collect(Collectors.toList()));

      return new PlanAndMappings(rewrittenTopN, mapping);
    }

    @Override
    public PlanAndMappings visitSort(SortNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);
      Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
      SymbolMapper mapper = symbolMapper(mapping);

      OrderingScheme newOrderingScheme = mapper.map(node.getOrderingScheme());

      return new PlanAndMappings(
          new SortNode(
              node.getPlanNodeId(),
              rewrittenSource.getRoot(),
              newOrderingScheme,
              node.isPartial(),
              node.isOrderByAllIdsAndTime()),
          mapping);
    }

    @Override
    public PlanAndMappings visitFilter(FilterNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);
      Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
      SymbolMapper mapper = symbolMapper(mapping);

      Expression newPredicate = mapper.map(node.getPredicate());

      return new PlanAndMappings(
          new FilterNode(node.getPlanNodeId(), rewrittenSource.getRoot(), newPredicate), mapping);
    }

    @Override
    public PlanAndMappings visitProject(ProjectNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);

      // Assignment of a form `s -> x` establishes new semantics for symbol s.
      // It is possible though that symbol `s` is present in the source plan, and represents the
      // same or different semantics.
      // As a consequence, any symbol mapping derived from the source plan involving symbol `s`
      // becomes potentially invalid,
      // e.g. `s -> y` or `y -> s` refer to the old semantics of symbol `s`.
      // In such case, the underlying mappings are only used to map projection assignments' values
      // to ensure consistency with the source plan.
      // They aren't used to map projection outputs to avoid:
      // - errors from duplicate assignments
      // - incorrect results from mixed semantics of symbols
      // Also, the underlying mappings aren't passed up the plan, and new mappings aren't derived
      // from projection assignments
      // (with the exception for "deduplicating" mappings for repeated assignments).
      // This can be thought of as a "cut-off" at the point of potentially changed semantics.
      // Note: the issue of ambiguous symbols does not apply to symbols involved in context
      // (correlation) mapping.
      // Those symbols are supposed to represent constant semantics throughout the plan.

      Assignments assignments = node.getAssignments();
      Set<Symbol> newlyAssignedSymbols =
          assignments.filter(output -> !assignments.isIdentity(output)).getSymbols();
      Set<Symbol> symbolsInSourceMapping =
          ImmutableSet.<Symbol>builder()
              .addAll(rewrittenSource.getMappings().keySet())
              .addAll(rewrittenSource.getMappings().values())
              .build();
      Set<Symbol> symbolsInCorrelationMapping =
          ImmutableSet.<Symbol>builder()
              .addAll(context.getCorrelationMapping().keySet())
              .addAll(context.getCorrelationMapping().values())
              .build();
      boolean ambiguousSymbolsPresent =
          !Sets.intersection(
                  newlyAssignedSymbols,
                  Sets.difference(symbolsInSourceMapping, symbolsInCorrelationMapping))
              .isEmpty();

      Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
      SymbolMapper mapper = symbolMapper(mapping);

      // canonicalize ProjectNode assignments
      ImmutableList.Builder<Map.Entry<Symbol, Expression>> rewrittenAssignments =
          ImmutableList.builder();
      for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
        rewrittenAssignments.add(
            new SimpleEntry<>(
                ambiguousSymbolsPresent ? assignment.getKey() : mapper.map(assignment.getKey()),
                mapper.map(assignment.getValue())));
      }

      // deduplicate assignments
      Map<Symbol, Expression> deduplicateAssignments =
          rewrittenAssignments.build().stream()
              .distinct()
              .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

      // derive new mappings for ProjectNode output symbols
      Map<Symbol, Symbol> newMapping =
          mappingFromAssignments(deduplicateAssignments, ambiguousSymbolsPresent);

      Map<Symbol, Symbol> outputMapping = new HashMap<>();
      outputMapping.putAll(ambiguousSymbolsPresent ? context.getCorrelationMapping() : mapping);
      outputMapping.putAll(newMapping);

      mapper = symbolMapper(outputMapping);

      // build new Assignments with canonical outputs
      // duplicate entries will be removed by the Builder
      Assignments.Builder newAssignments = Assignments.builder();
      for (Map.Entry<Symbol, Expression> assignment : deduplicateAssignments.entrySet()) {
        newAssignments.put(mapper.map(assignment.getKey()), assignment.getValue());
      }

      return new PlanAndMappings(
          new ProjectNode(node.getPlanNodeId(), rewrittenSource.getRoot(), newAssignments.build()),
          outputMapping);
    }

    private Map<Symbol, Symbol> mappingFromAssignments(
        Map<Symbol, Expression> assignments, boolean ambiguousSymbolsPresent) {
      Map<Symbol, Symbol> newMapping = new HashMap<>();
      Map<Expression, Symbol> inputsToOutputs = new HashMap<>();
      for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
        Expression expression = assignment.getValue();
        // 1. for trivial symbol projection, map output symbol to input symbol
        // If the assignment potentially introduces a reused (ambiguous) symbol, do not map output
        // to input
        // to avoid mixing semantics. Input symbols represent semantics as in the source plan,
        // while output symbols represent newly established semantics.
        if (expression instanceof SymbolReference && !ambiguousSymbolsPresent) {
          Symbol value = Symbol.from(expression);
          if (!assignment.getKey().equals(value)) {
            newMapping.put(assignment.getKey(), value);
          }
        }
        // 2. map same deterministic expressions within a projection into the same symbol
        // omit NullLiterals since those have ambiguous types
        else if (DeterminismEvaluator.isDeterministic(expression)
            && !(expression instanceof NullLiteral)) {
          Symbol previous = inputsToOutputs.get(expression);
          if (previous == null) {
            inputsToOutputs.put(expression, assignment.getKey());
          } else {
            newMapping.put(assignment.getKey(), previous);
          }
        }
      }
      return newMapping;
    }

    @Override
    public PlanAndMappings visitOutput(OutputNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getChild().accept(this, context);
      Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
      SymbolMapper mapper = symbolMapper(mapping);

      List<Symbol> newOutputs = mapper.map(node.getOutputSymbols());

      return new PlanAndMappings(
          new OutputNode(
              node.getPlanNodeId(), rewrittenSource.getRoot(), node.getColumnNames(), newOutputs),
          mapping);
    }

    @Override
    public PlanAndMappings visitEnforceSingleRow(
        EnforceSingleRowNode node, UnaliasContext context) {
      PlanAndMappings rewrittenSource = node.getSource().accept(this, context);

      return new PlanAndMappings(
          node.replaceChildren(ImmutableList.of(rewrittenSource.getRoot())),
          rewrittenSource.getMappings());
    }

    @Override
    public PlanAndMappings visitApply(ApplyNode node, UnaliasContext context) {
      // it is assumed that apart from correlation (and possibly outer correlation), symbols are
      // distinct between Input and Subquery
      // rewrite Input
      PlanAndMappings rewrittenInput = node.getInput().accept(this, context);
      Map<Symbol, Symbol> inputMapping = new HashMap<>(rewrittenInput.getMappings());
      SymbolMapper mapper = symbolMapper(inputMapping);

      // rewrite correlation with mapping from Input
      List<Symbol> rewrittenCorrelation = mapper.mapAndDistinct(node.getCorrelation());

      // extract new mappings for correlation symbols to apply in Subquery
      Set<Symbol> correlationSymbols = ImmutableSet.copyOf(node.getCorrelation());
      Map<Symbol, Symbol> correlationMapping = new HashMap<>();
      for (Map.Entry<Symbol, Symbol> entry : inputMapping.entrySet()) {
        if (correlationSymbols.contains(entry.getKey())) {
          correlationMapping.put(entry.getKey(), mapper.map(entry.getKey()));
        }
      }

      Map<Symbol, Symbol> mappingForSubquery = new HashMap<>();
      mappingForSubquery.putAll(context.getCorrelationMapping());
      mappingForSubquery.putAll(correlationMapping);

      // rewrite Subquery
      PlanAndMappings rewrittenSubquery =
          node.getSubquery().accept(this, new UnaliasContext(mappingForSubquery));

      // unify mappings from Input and Subquery to rewrite Subquery assignments
      Map<Symbol, Symbol> resultMapping = new HashMap<>();
      resultMapping.putAll(rewrittenInput.getMappings());
      resultMapping.putAll(rewrittenSubquery.getMappings());
      mapper = symbolMapper(resultMapping);

      ImmutableList.Builder<Map.Entry<Symbol, ApplyNode.SetExpression>> rewrittenAssignments =
          ImmutableList.builder();
      for (Map.Entry<Symbol, ApplyNode.SetExpression> assignment :
          node.getSubqueryAssignments().entrySet()) {
        rewrittenAssignments.add(
            new SimpleEntry<>(mapper.map(assignment.getKey()), mapper.map(assignment.getValue())));
      }

      // deduplicate assignments
      Map<Symbol, ApplyNode.SetExpression> deduplicateAssignments =
          rewrittenAssignments.build().stream()
              .distinct()
              .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

      mapper = symbolMapper(resultMapping);

      // build new Assignments with canonical outputs
      // duplicate entries will be removed by the Builder
      ImmutableMap.Builder<Symbol, ApplyNode.SetExpression> newAssignments = ImmutableMap.builder();
      for (Map.Entry<Symbol, ApplyNode.SetExpression> assignment :
          deduplicateAssignments.entrySet()) {
        newAssignments.put(mapper.map(assignment.getKey()), assignment.getValue());
      }

      return new PlanAndMappings(
          new ApplyNode(
              node.getPlanNodeId(),
              rewrittenInput.getRoot(),
              rewrittenSubquery.getRoot(),
              newAssignments.buildOrThrow(),
              rewrittenCorrelation,
              node.getOriginSubquery()),
          resultMapping);
    }

    @Override
    public PlanAndMappings visitCorrelatedJoin(CorrelatedJoinNode node, UnaliasContext context) {
      // it is assumed that apart from correlation (and possibly outer correlation), symbols are
      // distinct between left and right CorrelatedJoin source
      // rewrite Input
      PlanAndMappings rewrittenInput = node.getInput().accept(this, context);
      Map<Symbol, Symbol> inputMapping = new HashMap<>(rewrittenInput.getMappings());
      SymbolMapper mapper = symbolMapper(inputMapping);

      // rewrite correlation with mapping from Input
      List<Symbol> rewrittenCorrelation = mapper.mapAndDistinct(node.getCorrelation());

      // extract new mappings for correlation symbols to apply in Subquery
      Set<Symbol> correlationSymbols = ImmutableSet.copyOf(node.getCorrelation());
      Map<Symbol, Symbol> correlationMapping = new HashMap<>();
      for (Map.Entry<Symbol, Symbol> entry : inputMapping.entrySet()) {
        if (correlationSymbols.contains(entry.getKey())) {
          correlationMapping.put(entry.getKey(), mapper.map(entry.getKey()));
        }
      }

      Map<Symbol, Symbol> mappingForSubquery = new HashMap<>();
      mappingForSubquery.putAll(context.getCorrelationMapping());
      mappingForSubquery.putAll(correlationMapping);

      // rewrite Subquery
      PlanAndMappings rewrittenSubquery =
          node.getSubquery().accept(this, new UnaliasContext(mappingForSubquery));

      // unify mappings from Input and Subquery
      Map<Symbol, Symbol> resultMapping = new HashMap<>();
      resultMapping.putAll(rewrittenInput.getMappings());
      resultMapping.putAll(rewrittenSubquery.getMappings());

      // rewrite filter with unified mapping
      mapper = symbolMapper(resultMapping);
      Expression newFilter = mapper.map(node.getFilter());

      return new PlanAndMappings(
          new CorrelatedJoinNode(
              node.getPlanNodeId(),
              rewrittenInput.getRoot(),
              rewrittenSubquery.getRoot(),
              rewrittenCorrelation,
              node.getJoinType(),
              newFilter,
              node.getOriginSubquery()),
          resultMapping);
    }

    @Override
    public PlanAndMappings visitJoin(JoinNode node, UnaliasContext context) {
      // it is assumed that symbols are distinct between left and right join source. Only symbols
      // from outer correlation might be the exception
      PlanAndMappings rewrittenLeft = node.getLeftChild().accept(this, context);
      PlanAndMappings rewrittenRight = node.getRightChild().accept(this, context);

      // unify mappings from left and right join source
      Map<Symbol, Symbol> unifiedMapping = new HashMap<>();
      unifiedMapping.putAll(rewrittenLeft.getMappings());
      unifiedMapping.putAll(rewrittenRight.getMappings());

      SymbolMapper mapper = symbolMapper(unifiedMapping);

      ImmutableList.Builder<JoinNode.EquiJoinClause> builder = ImmutableList.builder();
      for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
        builder.add(
            new JoinNode.EquiJoinClause(
                mapper.map(clause.getLeft()), mapper.map(clause.getRight())));
      }
      List<JoinNode.EquiJoinClause> newCriteria = builder.build();

      Optional<Expression> newFilter = node.getFilter().map(mapper::map);

      // derive new mappings from inner join equi criteria
      Map<Symbol, Symbol> newMapping = new HashMap<>();
      if (node.getJoinType() == INNER) {
        newCriteria
            // Map right equi-condition symbol to left symbol. This helps to
            // reuse join node partitioning better as partitioning properties are
            // only derived from probe side symbols
            .forEach(clause -> newMapping.put(clause.getRight(), clause.getLeft()));
      }

      Map<Symbol, Symbol> outputMapping = new HashMap<>();
      outputMapping.putAll(unifiedMapping);
      outputMapping.putAll(newMapping);

      mapper = symbolMapper(outputMapping);
      List<Symbol> canonicalOutputs = mapper.mapAndDistinct(node.getOutputSymbols());
      List<Symbol> newLeftOutputSymbols =
          canonicalOutputs.stream()
              .filter(rewrittenLeft.getRoot().getOutputSymbols()::contains)
              .collect(toImmutableList());
      List<Symbol> newRightOutputSymbols =
          canonicalOutputs.stream()
              .filter(rewrittenRight.getRoot().getOutputSymbols()::contains)
              .collect(toImmutableList());

      return new PlanAndMappings(
          new JoinNode(
              node.getPlanNodeId(),
              node.getJoinType(),
              rewrittenLeft.getRoot(),
              rewrittenRight.getRoot(),
              newCriteria,
              newLeftOutputSymbols,
              newRightOutputSymbols,
              newFilter,
              node.isSpillable()),
          outputMapping);
    }
  }

  private static class UnaliasContext {
    // Correlation mapping is a record of how correlation symbols have been mapped in the subplan
    // which provides them.
    // All occurrences of correlation symbols within the correlated subquery must be remapped
    // accordingly.
    // In case of nested correlation, correlationMappings has required mappings for correlation
    // symbols from all levels of nesting.
    private final Map<Symbol, Symbol> correlationMapping;

    public UnaliasContext(Map<Symbol, Symbol> correlationMapping) {
      this.correlationMapping = requireNonNull(correlationMapping, "correlationMapping is null");
    }

    public static UnaliasContext empty() {
      return new UnaliasContext(ImmutableMap.of());
    }

    public Map<Symbol, Symbol> getCorrelationMapping() {
      return correlationMapping;
    }
  }

  private static class PlanAndMappings {
    private final PlanNode root;
    private final Map<Symbol, Symbol> mappings;

    public PlanAndMappings(PlanNode root, Map<Symbol, Symbol> mappings) {
      this.root = requireNonNull(root, "root is null");
      this.mappings = ImmutableMap.copyOf(requireNonNull(mappings, "mappings is null"));
    }

    public PlanNode getRoot() {
      return root;
    }

    public Map<Symbol, Symbol> getMappings() {
      return mappings;
    }
  }
}
