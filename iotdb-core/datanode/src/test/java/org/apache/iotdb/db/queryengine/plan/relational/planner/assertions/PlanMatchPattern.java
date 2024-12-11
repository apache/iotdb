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

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.GroupReference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_FIRST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_FIRST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.StrictAssignedSymbolsMatcher.actualAssignments;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.StrictSymbolsMatcher.actualOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.NullOrdering.FIRST;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.NullOrdering.UNDEFINED;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.Ordering.ASCENDING;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.Ordering.DESCENDING;

public final class PlanMatchPattern {
  private final List<Matcher> matchers = new ArrayList<>();

  private final List<PlanMatchPattern> sourcePatterns;
  private boolean anyTree;

  public static PlanMatchPattern node(
      Class<? extends PlanNode> nodeClass, PlanMatchPattern... sources) {
    return any(sources).with(new PlanNodeMatcher(nodeClass));
  }

  public static PlanMatchPattern any(PlanMatchPattern... sources) {
    return new PlanMatchPattern(ImmutableList.copyOf(sources));
  }

  /**
   * Matches to any tree of nodes with children matching to given source matchers.
   * anyTree(tableScan("nation")) - will match to any plan which all leafs contain any node
   * containing table scan from nation table.
   *
   * <p>Note: anyTree does not match zero nodes. E.g. output(anyTree(tableScan)) will NOT match
   * TableScan node followed by OutputNode.
   */
  public static PlanMatchPattern anyTree(PlanMatchPattern... sources) {
    return any(sources).matchToAnyNodeTree();
  }

  public static PlanMatchPattern anyNot(
      Class<? extends PlanNode> excludeNodeClass, PlanMatchPattern... sources) {
    return any(sources).with(new NotPlanNodeMatcher(excludeNodeClass));
  }

  /*public static PlanMatchPattern remoteSource(List<PlanFragmentId> sourceFragmentIds)
  {
      return node(RemoteSourceNode.class)
              .with(new RemoteSourceMatcher(
                      sourceFragmentIds,
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty()));
  }*/
  public static PlanMatchPattern infoSchemaTableScan(
      String expectedTableName, Optional<Integer> dataNodeId) {
    return node(InformationSchemaTableScanNode.class)
        .with(
            new InformationSchemaTableScanMatcher(
                expectedTableName,
                Optional.empty(),
                Collections.emptyList(),
                Collections.emptySet(),
                dataNodeId));
  }

  public static PlanMatchPattern tableScan(String expectedTableName) {
    return node(DeviceTableScanNode.class)
        .with(
            new DeviceTableScanMatcher(
                expectedTableName,
                Optional.empty(),
                Collections.emptyList(),
                Collections.emptySet()));
  }

  public static PlanMatchPattern tableScan(
      String expectedTableName, List<String> outputSymbols, Set<String> assignmentsKeys) {
    PlanMatchPattern pattern =
        node(DeviceTableScanNode.class)
            .with(
                new DeviceTableScanMatcher(
                    expectedTableName, Optional.empty(), outputSymbols, assignmentsKeys));
    outputSymbols.forEach(
        symbol -> pattern.withAlias(symbol, new ColumnReference(expectedTableName, symbol)));
    return pattern;
  }

  public static PlanMatchPattern tableScan(
      String expectedTableName, Map<String, String> columnReferences) {
    PlanMatchPattern result = tableScan(expectedTableName);
    return result.addColumnReferences(expectedTableName, columnReferences);
  }

  public static PlanMatchPattern strictTableScan(
      String expectedTableName, Map<String, String> columnReferences, List<String> outputs) {
    return tableScan(expectedTableName, columnReferences).withExactOutputs(outputs);
  }

  private PlanMatchPattern addColumnReferences(
      String expectedTableName, Map<String, String> columnReferences) {
    columnReferences
        .entrySet()
        .forEach(
            reference ->
                withAlias(
                    reference.getKey(), columnReference(expectedTableName, reference.getValue())));
    return this;
  }

  public static PlanMatchPattern aggregation(
      Map<String, ExpectedValueProvider<AggregationFunction>> aggregations,
      PlanMatchPattern source) {
    PlanMatchPattern result = node(AggregationNode.class, source);
    aggregations
        .entrySet()
        .forEach(
            aggregation ->
                result.withAlias(
                    aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
    return result;
  }

  public static PlanMatchPattern aggregation(
      Map<String, ExpectedValueProvider<AggregationFunction>> aggregations,
      AggregationNode.Step step,
      PlanMatchPattern source) {
    PlanMatchPattern result =
        node(AggregationNode.class, source).with(new AggregationStepMatcher(step));
    aggregations
        .entrySet()
        .forEach(
            aggregation ->
                result.withAlias(
                    aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
    return result;
  }

  public static PlanMatchPattern aggregation(
      Map<String, ExpectedValueProvider<AggregationFunction>> aggregations,
      Predicate<AggregationNode> predicate,
      PlanMatchPattern source) {
    PlanMatchPattern result =
        node(AggregationNode.class, source).with(new PredicateMatcher<>(predicate));
    aggregations
        .entrySet()
        .forEach(
            aggregation ->
                result.withAlias(
                    aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
    return result;
  }

  public static PlanMatchPattern aggregation(
      GroupingSetDescriptor groupingSets,
      Map<Optional<String>, ExpectedValueProvider<AggregationFunction>> aggregations,
      Optional<Symbol> groupId,
      AggregationNode.Step step,
      PlanMatchPattern source) {
    return aggregation(groupingSets, aggregations, ImmutableList.of(), groupId, step, source);
  }

  public static PlanMatchPattern aggregation(
      GroupingSetDescriptor groupingSets,
      Map<Optional<String>, ExpectedValueProvider<AggregationFunction>> aggregations,
      List<String> preGroupedSymbols,
      Optional<Symbol> groupId,
      AggregationNode.Step step,
      PlanMatchPattern source) {
    return aggregation(
        groupingSets, aggregations, preGroupedSymbols, ImmutableList.of(), groupId, step, source);
  }

  public static PlanMatchPattern aggregation(
      GroupingSetDescriptor groupingSets,
      Map<Optional<String>, ExpectedValueProvider<AggregationFunction>> aggregations,
      List<String> preGroupedSymbols,
      List<String> masks,
      Optional<Symbol> groupId,
      AggregationNode.Step step,
      PlanMatchPattern source) {
    PlanMatchPattern result =
        node(AggregationNode.class, source)
            .with(new AggregationMatcher(groupingSets, preGroupedSymbols, masks, groupId, step));
    aggregations
        .entrySet()
        .forEach(
            aggregation ->
                result.withAlias(
                    aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
    return result;
  }

  public static ExpectedValueProvider<AggregationFunction> aggregationFunction(
      String name, List<String> args) {
    return new AggregationFunctionProvider(
        name, false, toSymbolAliases(args), ImmutableList.of(), Optional.empty());
  }

  public static ExpectedValueProvider<AggregationFunction> aggregationFunction(
      String name, List<String> args, List<PlanMatchPattern.Ordering> orderBy) {
    return new AggregationFunctionProvider(
        name, false, toSymbolAliases(args), orderBy, Optional.empty());
  }

  public static ExpectedValueProvider<AggregationFunction> aggregationFunction(
      String name, boolean distinct, List<PlanTestSymbol> args) {
    return new AggregationFunctionProvider(
        name, distinct, args, ImmutableList.of(), Optional.empty());
  }

  // Attention: Now we only pass aliases according to outputSymbols, but we don't verify the output
  // column if exists in Table and their order because there maybe partial Agg-result.
  public static PlanMatchPattern aggregationTableScan(
      GroupingSetDescriptor groupingSets,
      List<String> preGroupedSymbols,
      Optional<Symbol> groupId,
      AggregationNode.Step step,
      String expectedTableName,
      List<String> outputSymbols,
      Set<String> assignmentsKeys) {
    PlanMatchPattern result = node(AggregationTableScanNode.class);

    result.with(
        new AggregationDeviceTableScanMatcher(
            groupingSets,
            preGroupedSymbols,
            ImmutableList.of(),
            groupId,
            step,
            expectedTableName,
            Optional.empty(),
            outputSymbols,
            assignmentsKeys));

    outputSymbols.forEach(
        outputSymbol ->
            result.withAlias(outputSymbol, new ColumnReference(expectedTableName, outputSymbol)));
    return result;
  }

  /*
  public static PlanMatchPattern distinctLimit(long limit, List<String> distinctSymbols, PlanMatchPattern source)
  {
      return node(DistinctLimitNode.class, source).with(new DistinctLimitMatcher(
              limit,
              toSymbolAliases(distinctSymbols),
              Optional.empty()));
  }

  public static PlanMatchPattern distinctLimit(long limit, List<String> distinctSymbols, String hashSymbol, PlanMatchPattern source)
  {
      return node(DistinctLimitNode.class, source).with(new DistinctLimitMatcher(
              limit,
              toSymbolAliases(distinctSymbols),
              Optional.of(new SymbolAlias(hashSymbol))));
  }

  public static PlanMatchPattern markDistinct(
          String markerSymbol,
          List<String> distinctSymbols,
          PlanMatchPattern source)
  {
      return node(MarkDistinctNode.class, source).with(new MarkDistinctMatcher(
              new SymbolAlias(markerSymbol),
              toSymbolAliases(distinctSymbols),
              Optional.empty()));
  }

  public static PlanMatchPattern markDistinct(
          String markerSymbol,
          List<String> distinctSymbols,
          String hashSymbol,
          PlanMatchPattern source)
  {
      return node(MarkDistinctNode.class, source).with(new MarkDistinctMatcher(
              new SymbolAlias(markerSymbol),
              toSymbolAliases(distinctSymbols),
              Optional.of(new SymbolAlias(hashSymbol))));
  }

  public static PlanMatchPattern window(Consumer<WindowMatcher.Builder> handler, PlanMatchPattern source)
  {
      WindowMatcher.Builder builder = new WindowMatcher.Builder(source);
      handler.accept(builder);
      return builder.build();
  }

  public static PlanMatchPattern rowNumber(Consumer<RowNumberMatcher.Builder> handler, PlanMatchPattern source)
  {
      RowNumberMatcher.Builder builder = new RowNumberMatcher.Builder(source);
      handler.accept(builder);
      return builder.build();
  }

  public static PlanMatchPattern topNRanking(Consumer<TopNRankingMatcher.Builder> handler, PlanMatchPattern source)
  {
      TopNRankingMatcher.Builder builder = new TopNRankingMatcher.Builder(source);
      handler.accept(builder);
      return builder.build();
  }

  public static PlanMatchPattern patternRecognition(Consumer<PatternRecognitionMatcher.Builder> handler, PlanMatchPattern source)
  {
      PatternRecognitionMatcher.Builder builder = new PatternRecognitionMatcher.Builder(source);
      handler.accept(builder);
      return builder.build();
  }*/

  public static PlanMatchPattern join(
      JoinNode.JoinType type, Consumer<JoinMatcher.Builder> handler) {
    JoinMatcher.Builder builder = new JoinMatcher.Builder(type);
    handler.accept(builder);
    return builder.build();
  }

  public static PlanMatchPattern sort(PlanMatchPattern source) {
    return node(SortNode.class, source);
  }

  public static PlanMatchPattern sort(List<Ordering> orderBy, PlanMatchPattern source) {
    return node(SortNode.class, source).with(new SortMatcher(orderBy));
  }

  public static PlanMatchPattern streamSort(List<Ordering> orderBy, PlanMatchPattern source) {
    return node(StreamSortNode.class, source).with(new SortMatcher(orderBy));
  }

  /*public static PlanMatchPattern topN(long count, List<Ordering> orderBy, PlanMatchPattern source)
  {
      return topN(count, orderBy, TopNNode.Step.SINGLE, source);
  }

  public static PlanMatchPattern topN(long count, List<Ordering> orderBy, TopNNode.Step step, PlanMatchPattern source)
  {
      return node(TopNNode.class, source).with(new TopNMatcher(count, orderBy, step));
  }*/

  public static PlanMatchPattern output(PlanMatchPattern source) {
    return node(OutputNode.class, source);
  }

  public static PlanMatchPattern output(List<String> outputs, PlanMatchPattern source) {
    PlanMatchPattern result = output(source);
    result.withOutputs(outputs);
    return result;
  }

  public static PlanMatchPattern strictOutput(List<String> outputs, PlanMatchPattern source) {
    return output(outputs, source).withExactOutputs(outputs);
  }

  public static PlanMatchPattern project(PlanMatchPattern source) {
    return node(ProjectNode.class, source);
  }

  public static PlanMatchPattern project(
      Map<String, ExpressionMatcher> assignments, PlanMatchPattern source) {
    PlanMatchPattern result = project(source);
    assignments
        .entrySet()
        .forEach(assignment -> result.withAlias(assignment.getKey(), assignment.getValue()));
    return result;
  }

  public static PlanMatchPattern identityProject(PlanMatchPattern source) {
    return node(ProjectNode.class, source).with(new IdentityProjectionMatcher());
  }

  public static PlanMatchPattern strictProject(
      Map<String, ExpressionMatcher> assignments, PlanMatchPattern source) {
    /*
     * Under the current implementation of project, all of the outputs are also in the assignment.
     * If the implementation changes, this will need to change too.
     */
    return project(assignments, source)
        .withExactAssignedOutputs(assignments.values())
        .withExactAssignments(assignments.values());
  }

  public static PlanMatchPattern exchange(PlanMatchPattern... sources) {
    return node(ExchangeNode.class, sources);
  }

  public static ExpectedValueProvider<JoinNode.EquiJoinClause> equiJoinClause(
      String left, String right) {
    return new EquiJoinClauseProvider(new SymbolAlias(left), new SymbolAlias(right));
  }

  public static SymbolAlias symbol(String alias) {
    return new SymbolAlias(alias);
  }

  public static PlanMatchPattern filter(Expression expectedPredicate, PlanMatchPattern source) {
    return node(FilterNode.class, source).with(new FilterMatcher(expectedPredicate));
  }

  public static PlanMatchPattern filter(PlanMatchPattern source) {
    return node(FilterNode.class, source);
  }

  /*public static PlanMatchPattern groupId(List<List<String>> groupingSets, String groupIdSymbol, PlanMatchPattern source)
      {
          return groupId(groupingSets, ImmutableList.of(), groupIdSymbol, source);
      }

      public static PlanMatchPattern groupId(
              List<List<String>> groupingSets,
              List<String> aggregationArguments,
              String groupIdSymbol,
              PlanMatchPattern source)
      {
          return groupId(groupingSets, ImmutableMap.of(), aggregationArguments, groupIdSymbol, source);
      }

      public static PlanMatchPattern groupId(
              List<List<String>> groupingSets,
              Map<String, String> groupingColumns,
              List<String> aggregationArguments,
              String groupIdSymbol,
              PlanMatchPattern source)
      {
          return node(GroupIdNode.class, source).with(new GroupIdMatcher(
                  groupingSets,
                  groupingColumns,
                  aggregationArguments,
                  groupIdSymbol));
      }

      public static PlanMatchPattern values(
              Map<String, Integer> aliasToIndex,
              Optional<Integer> expectedOutputSymbolCount,
              Optional<List<Expression>> expectedRows)
      {
          return node(ValuesNode.class).with(new ValuesMatcher(aliasToIndex, expectedOutputSymbolCount, expectedRows));
      }

      private static PlanMatchPattern values(List<String> aliases, Optional<List<List<Expression>>> expectedRows)
      {
          return values(
                  aliasToIndex(aliases),
                  Optional.of(aliases.size()),
                  expectedRows.map(list -> list.stream()
                          .map(Row::new)
                          .collect(toImmutableList())));
      }

      public static Map<String, Integer> aliasToIndex(List<String> aliases)
      {
          return Maps.uniqueIndex(IntStream.range(0, aliases.size()).boxed().iterator(), aliases::get);
      }

      public static PlanMatchPattern values(Map<String, Integer> aliasToIndex)
      {
          return values(aliasToIndex, Optional.empty(), Optional.empty());
      }

      public static PlanMatchPattern values(String... aliases)
      {
          return values(ImmutableList.copyOf(aliases));
      }

      public static PlanMatchPattern values(int rowCount)
      {
          return values(ImmutableList.of(), nCopies(rowCount, ImmutableList.of()));
      }

      public static PlanMatchPattern values(List<String> aliases, List<List<Expression>> expectedRows)
      {
          return values(aliases, Optional.of(expectedRows));
      }

      public static PlanMatchPattern values(List<String> aliases)
      {
          return values(aliases, Optional.empty());
      }
  */
  public static PlanMatchPattern offset(long rowCount, PlanMatchPattern source) {
    return node(OffsetNode.class, source).with(new OffsetMatcher(rowCount));
  }

  public static PlanMatchPattern limit(long limit, PlanMatchPattern source) {
    return limit(limit, ImmutableList.of(), source);
  }

  public static PlanMatchPattern limit(
      long limit, List<Ordering> tiesResolvers, PlanMatchPattern source) {
    return limit(limit, tiesResolvers, ImmutableList.of(), source);
  }

  public static PlanMatchPattern limit(
      long limit,
      List<Ordering> tiesResolvers,
      List<String> preSortedInputs,
      PlanMatchPattern source) {
    return node(LimitNode.class, source)
        .with(
            new LimitMatcher(
                limit,
                tiesResolvers,
                preSortedInputs.stream().map(SymbolAlias::new).collect(toImmutableList())));
  }

  public static PlanMatchPattern mergeSort(PlanMatchPattern... sources) {
    return node(MergeSortNode.class, sources);
  }

  public static PlanMatchPattern collect(PlanMatchPattern... sources) {
    return node(CollectNode.class, sources);
  }

  public static PlanMatchPattern exchange() {
    return node(ExchangeNode.class).with(new ExchangeNodeMatcher());
  }

  public static PlanMatchPattern enforceSingleRow(PlanMatchPattern source) {
    return node(EnforceSingleRowNode.class, source);
  }

  public PlanMatchPattern(List<PlanMatchPattern> sourcePatterns) {
    requireNonNull(sourcePatterns, "sourcePatterns are null");

    this.sourcePatterns = ImmutableList.copyOf(sourcePatterns);
  }

  List<PlanMatchingState> shapeMatches(PlanNode node) {
    ImmutableList.Builder<PlanMatchingState> states = ImmutableList.builder();
    if (anyTree) {
      int sourcesCount = node.getChildren().size();
      if (sourcesCount > 1) {
        states.add(new PlanMatchingState(nCopies(sourcesCount, this)));
      } else {
        states.add(new PlanMatchingState(ImmutableList.of(this)));
      }
    }
    if (node instanceof GroupReference) {
      if (sourcePatterns.isEmpty() && shapeMatchesMatchers(node)) {
        states.add(new PlanMatchingState(ImmutableList.of()));
      }
    } else if (node.getChildren().size() == sourcePatterns.size() && shapeMatchesMatchers(node)) {
      states.add(new PlanMatchingState(sourcePatterns));
    }
    return states.build();
  }

  private boolean shapeMatchesMatchers(PlanNode node) {
    return matchers.stream().allMatch(it -> it.shapeMatches(node));
  }

  MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    SymbolAliases.Builder newAliases = SymbolAliases.builder();

    for (Matcher matcher : matchers) {
      MatchResult matchResult = matcher.detailMatches(node, sessionInfo, metadata, symbolAliases);
      if (!matchResult.isMatch()) {
        return NO_MATCH;
      }
      newAliases.putAll(matchResult.getAliases());
    }

    return match(newAliases.build());
  }

  public <T extends PlanNode> PlanMatchPattern with(Class<T> clazz, Predicate<T> predicate) {
    return with(
        new Matcher() {
          @Override
          public boolean shapeMatches(PlanNode node) {
            return clazz.isInstance(node);
          }

          @Override
          public MatchResult detailMatches(
              PlanNode node,
              SessionInfo sessionInfo,
              Metadata metadata,
              SymbolAliases symbolAliases) {
            if (predicate.test(clazz.cast(node))) {
              return match();
            }

            return NO_MATCH;
          }
        });
  }

  public PlanMatchPattern with(Matcher matcher) {
    matchers.add(matcher);
    return this;
  }

  public PlanMatchPattern withAlias(String alias) {
    return withAlias(Optional.of(alias), new AliasPresent(alias));
  }

  public PlanMatchPattern withAlias(String alias, RvalueMatcher matcher) {
    return withAlias(Optional.of(alias), matcher);
  }

  public PlanMatchPattern withAlias(Optional<String> alias, RvalueMatcher matcher) {
    matchers.add(new AliasMatcher(alias, matcher));
    return this;
  }

  /*
   * This is useful if you already know the bindings for the aliases you expect to find
   * in the outputs. This is the case for symbols that are produced by a direct or indirect
   * source of the node you're applying this to.
   */
  public PlanMatchPattern withExactOutputs(String... expectedAliases) {
    return withExactOutputs(ImmutableList.copyOf(expectedAliases));
  }

  public PlanMatchPattern withExactOutputs(List<String> expectedAliases) {
    matchers.add(new StrictSymbolsMatcher(actualOutputs(), expectedAliases));
    return this;
  }

  public PlanMatchPattern withExactAssignedOutputs(
      Collection<? extends RvalueMatcher> expectedAliases) {
    matchers.add(new StrictAssignedSymbolsMatcher(actualOutputs(), expectedAliases));
    return this;
  }

  public PlanMatchPattern withExactAssignments(
      Collection<? extends RvalueMatcher> expectedAliases) {
    matchers.add(new StrictAssignedSymbolsMatcher(actualAssignments(), expectedAliases));
    return this;
  }

  public static RvalueMatcher columnReference(String tableName, String columnName) {
    return new ColumnReference(tableName, columnName);
  }

  public static DataType dataType(String sqlType) {
    SqlParser parser = new SqlParser();
    return parser.createType(sqlType, ZoneId.systemDefault());
  }

  public static ExpressionMatcher expression(Expression expression) {
    return new ExpressionMatcher(expression);
  }

  public PlanMatchPattern withOutputs(List<String> aliases) {
    matchers.add(new OutputMatcher(aliases));
    return this;
  }

  PlanMatchPattern matchToAnyNodeTree() {
    anyTree = true;
    return this;
  }

  public boolean isTerminated() {
    return sourcePatterns.isEmpty();
  }

  /*public static ExpectedValueProvider<AggregationFunction> aggregationFunction(String name, List<String> args)
  {
      return new AggregationFunctionProvider(
              name,
              false,
              toSymbolAliases(args),
              ImmutableList.of(),
              Optional.empty());
  }

  public static ExpectedValueProvider<AggregationFunction> aggregationFunction(String name, List<String> args, List<Ordering> orderBy)
  {
      return new AggregationFunctionProvider(
              name,
              false,
              toSymbolAliases(args),
              orderBy,
              Optional.empty());
  }

  public static ExpectedValueProvider<AggregationFunction> aggregationFunction(String name, boolean distinct, List<PlanTestSymbol> args)
  {
      return new AggregationFunctionProvider(
              name,
              distinct,
              args,
              ImmutableList.of(),
              Optional.empty());
  }

  public static ExpectedValueProvider<WindowFunction> windowFunction(String name, List<String> args, WindowNode.Frame frame)
  {
      return new WindowFunctionProvider(name, frame, toSymbolAliases(args));
  }*/

  public static List<Expression> toSymbolReferences(
      List<PlanTestSymbol> aliases, SymbolAliases symbolAliases) {
    return aliases.stream()
        .map(arg -> arg.toSymbol(symbolAliases).toSymbolReference())
        .collect(toImmutableList());
  }

  private static List<PlanTestSymbol> toSymbolAliases(List<String> aliases) {
    return aliases.stream().map(PlanMatchPattern::symbol).collect(toImmutableList());
  }

  public static Ordering sort(
      String field, SortItem.Ordering ordering, SortItem.NullOrdering nullOrdering) {
    return new Ordering(field, ordering, nullOrdering);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    toString(builder, 0);
    return builder.toString();
  }

  private void toString(StringBuilder builder, int indent) {
    checkState(matchers.stream().filter(PlanNodeMatcher.class::isInstance).count() <= 1);

    builder.append(indentString(indent)).append("- ");
    if (anyTree) {
      builder.append("anyTree");
    } else {
      builder.append("node");
    }

    Optional<PlanNodeMatcher> planNodeMatcher =
        matchers.stream()
            .filter(PlanNodeMatcher.class::isInstance)
            .map(PlanNodeMatcher.class::cast)
            .findFirst();

    planNodeMatcher.ifPresent(
        nodeMatcher ->
            builder.append("(").append(nodeMatcher.getNodeClass().getSimpleName()).append(")"));

    builder.append("\n");

    List<Matcher> matchersToPrint =
        matchers.stream()
            .filter(matcher -> !(matcher instanceof PlanNodeMatcher))
            .collect(toImmutableList());

    for (Matcher matcher : matchersToPrint) {
      builder
          .append(indentString(indent + 1))
          .append(matcher.toString().replace("\n", "\n" + indentString(indent + 1)))
          .append("\n");
    }

    for (PlanMatchPattern pattern : sourcePatterns) {
      pattern.toString(builder, indent + 1);
    }
  }

  private static String indentString(int indent) {
    return String.join("", Collections.nCopies(indent, "    "));
  }

  public static GroupingSetDescriptor globalAggregation() {
    return singleGroupingSet();
  }

  public static GroupingSetDescriptor singleGroupingSet(String... groupingKeys) {
    return singleGroupingSet(ImmutableList.copyOf(groupingKeys));
  }

  public static GroupingSetDescriptor singleGroupingSet(List<String> groupingKeys) {
    Set<Integer> globalGroupingSets;
    if (groupingKeys.size() == 0) {
      globalGroupingSets = ImmutableSet.of(0);
    } else {
      globalGroupingSets = ImmutableSet.of();
    }

    return new GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
  }

  public static class GroupingSetDescriptor {
    private final List<String> groupingKeys;
    private final int groupingSetCount;
    private final Set<Integer> globalGroupingSets;

    private GroupingSetDescriptor(
        List<String> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets) {
      this.groupingKeys = groupingKeys;
      this.groupingSetCount = groupingSetCount;
      this.globalGroupingSets = globalGroupingSets;
    }

    public List<String> getGroupingKeys() {
      return groupingKeys;
    }

    public int getGroupingSetCount() {
      return groupingSetCount;
    }

    public Set<Integer> getGlobalGroupingSets() {
      return globalGroupingSets;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("keys", groupingKeys)
          .add("count", groupingSetCount)
          .add("globalSets", globalGroupingSets)
          .toString();
    }
  }

  public static class Ordering {
    private final String field;
    private final SortItem.Ordering ordering;
    private final SortItem.NullOrdering nullOrdering;

    private Ordering(String field, SortItem.Ordering ordering, SortItem.NullOrdering nullOrdering) {
      this.field = field;
      this.ordering = ordering;
      this.nullOrdering = nullOrdering;
    }

    public String getField() {
      return field;
    }

    public SortOrder getSortOrder() {
      checkState(nullOrdering != UNDEFINED, "nullOrdering is undefined");
      if (ordering == ASCENDING) {
        if (nullOrdering == FIRST) {
          return ASC_NULLS_FIRST;
        }
        return ASC_NULLS_LAST;
      }
      checkState(ordering == DESCENDING);
      if (nullOrdering == FIRST) {
        return DESC_NULLS_FIRST;
      }
      return DESC_NULLS_LAST;
    }

    @Override
    public String toString() {
      String result = field + " " + ordering;
      if (nullOrdering != UNDEFINED) {
        result += " NULLS " + nullOrdering;
      }

      return result;
    }
  }
}
