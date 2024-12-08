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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AggregationNode extends SingleChildProcessNode {
  private final Map<Symbol, Aggregation> aggregations;
  private final GroupingSetDescriptor groupingSets;
  // indicate the pre sorted columns of GroupingKeys
  private List<Symbol> preGroupedSymbols;
  private final Step step;
  private final Optional<Symbol> hashSymbol;
  private final Optional<Symbol> groupIdSymbol;
  private final List<Symbol> outputs;

  public static AggregationNode singleAggregation(
      PlanNodeId id,
      PlanNode source,
      Map<Symbol, Aggregation> aggregations,
      GroupingSetDescriptor groupingSets) {
    return new AggregationNode(
        id,
        source,
        aggregations,
        groupingSets,
        ImmutableList.of(),
        Step.SINGLE,
        Optional.empty(),
        Optional.empty());
  }

  public AggregationNode(
      PlanNodeId id,
      PlanNode source,
      Map<Symbol, Aggregation> aggregations,
      GroupingSetDescriptor groupingSets,
      List<Symbol> preGroupedSymbols,
      Step step,
      Optional<Symbol> hashSymbol,
      Optional<Symbol> groupIdSymbol) {
    super(id);
    this.child = source;
    this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
    aggregations.values().forEach(aggregation -> aggregation.verifyArguments(step));

    requireNonNull(groupingSets, "groupingSets is null");
    groupIdSymbol.ifPresent(
        symbol ->
            checkArgument(
                groupingSets.getGroupingKeys().contains(symbol),
                "Grouping columns does not contain groupId column"));
    this.groupingSets = groupingSets;

    this.groupIdSymbol = requireNonNull(groupIdSymbol);

    boolean noOrderBy =
        aggregations.values().stream()
            .map(Aggregation::getOrderingScheme)
            .noneMatch(Optional::isPresent);
    checkArgument(
        noOrderBy || step == Step.SINGLE, "ORDER BY does not support distributed aggregation");

    this.step = step;
    this.hashSymbol = hashSymbol;

    requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
    checkArgument(
        preGroupedSymbols.isEmpty()
            || groupingSets.getGroupingKeys().containsAll(preGroupedSymbols),
        "Pre-grouped symbols must be a subset of the grouping keys");
    this.preGroupedSymbols = ImmutableList.copyOf(preGroupedSymbols);

    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    outputs.addAll(groupingSets.getGroupingKeys());
    hashSymbol.ifPresent(outputs::add);
    outputs.addAll(aggregations.keySet());

    this.outputs = outputs.build();
  }

  public List<Symbol> getGroupingKeys() {
    return groupingSets.getGroupingKeys();
  }

  public GroupingSetDescriptor getGroupingSets() {
    return groupingSets;
  }

  public void setPreGroupedSymbols(List<Symbol> preGroupedSymbols) {
    this.preGroupedSymbols = preGroupedSymbols;
  }

  /**
   * @return true if the aggregation collapses all rows into a single global group (e.g., as a
   *     result of a GROUP BY () query). Otherwise, false.
   */
  public boolean hasSingleGlobalAggregation() {
    return hasEmptyGroupingSet() && getGroupingSetCount() == 1;
  }

  /**
   * @return whether this node should produce default output in case of no input pages. For example
   *     for query:
   *     <p>SELECT count(*) FROM nation WHERE nationkey < 0
   *     <p>A default output of "0" is expected to be produced by FINAL aggregation operator.
   */
  public boolean hasDefaultOutput() {
    return hasEmptyGroupingSet() && (step.isOutputPartial() || step == Step.SINGLE);
  }

  public boolean hasEmptyGroupingSet() {
    return !groupingSets.getGlobalGroupingSets().isEmpty();
  }

  public boolean hasNonEmptyGroupingSet() {
    return groupingSets.getGroupingSetCount() > groupingSets.getGlobalGroupingSets().size();
  }

  @Override
  public PlanNode clone() {
    return new AggregationNode(
        id, null, aggregations, groupingSets, preGroupedSymbols, step, hashSymbol, groupIdSymbol);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputs;
  }

  public Map<Symbol, Aggregation> getAggregations() {
    return aggregations;
  }

  public List<Symbol> getPreGroupedSymbols() {
    return preGroupedSymbols;
  }

  public int getGroupingSetCount() {
    return groupingSets.getGroupingSetCount();
  }

  public Set<Integer> getGlobalGroupingSets() {
    return groupingSets.getGlobalGroupingSets();
  }

  public Optional<Symbol> getHashSymbol() {
    return hashSymbol;
  }

  public Optional<Symbol> getGroupIdSymbol() {
    return groupIdSymbol;
  }

  public Step getStep() {
    return this.step;
  }

  public boolean hasOrderings() {
    return aggregations.values().stream()
        .map(Aggregation::getOrderingScheme)
        .anyMatch(Optional::isPresent);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAggregation(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AggregationNode that = (AggregationNode) o;
    return Objects.equals(aggregations, that.aggregations)
        && Objects.equals(groupingSets, that.groupingSets)
        && Objects.equals(step, that.step)
        && Objects.equals(hashSymbol, that.hashSymbol)
        && Objects.equals(groupIdSymbol, that.groupIdSymbol);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), aggregations, groupingSets, step, hashSymbol, groupIdSymbol);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_AGGREGATION_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(aggregations.size(), byteBuffer);
    aggregations.forEach(
        (k, v) -> {
          Symbol.serialize(k, byteBuffer);
          v.serialize(byteBuffer);
        });
    groupingSets.serialize(byteBuffer);
    ReadWriteIOUtils.write(preGroupedSymbols.size(), byteBuffer);
    for (Symbol preGroupedSymbol : preGroupedSymbols) {
      Symbol.serialize(preGroupedSymbol, byteBuffer);
    }
    step.serialize(byteBuffer);
    ReadWriteIOUtils.write(hashSymbol.isPresent(), byteBuffer);
    if (hashSymbol.isPresent()) {
      Symbol.serialize(hashSymbol.get(), byteBuffer);
    }
    ReadWriteIOUtils.write(groupIdSymbol.isPresent(), byteBuffer);
    if (groupIdSymbol.isPresent()) {
      Symbol.serialize(groupIdSymbol.get(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_AGGREGATION_NODE.serialize(stream);
    ReadWriteIOUtils.write(aggregations.size(), stream);
    for (Map.Entry<Symbol, Aggregation> aggregation : aggregations.entrySet()) {
      Symbol.serialize(aggregation.getKey(), stream);
      aggregation.getValue().serialize(stream);
    }
    groupingSets.serialize(stream);
    ReadWriteIOUtils.write(preGroupedSymbols.size(), stream);
    for (Symbol preGroupedSymbol : preGroupedSymbols) {
      Symbol.serialize(preGroupedSymbol, stream);
    }
    step.serialize(stream);
    ReadWriteIOUtils.write(hashSymbol.isPresent(), stream);
    if (hashSymbol.isPresent()) {
      Symbol.serialize(hashSymbol.get(), stream);
    }
    ReadWriteIOUtils.write(groupIdSymbol.isPresent(), stream);
    if (groupIdSymbol.isPresent()) {
      Symbol.serialize(groupIdSymbol.get(), stream);
    }
  }

  public static AggregationNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    final Map<Symbol, Aggregation> aggregations = new LinkedHashMap<>(size);
    while (size-- > 0) {
      aggregations.put(Symbol.deserialize(byteBuffer), Aggregation.deserialize(byteBuffer));
    }
    GroupingSetDescriptor groupingSetDescriptor = GroupingSetDescriptor.deserialize(byteBuffer);
    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> preGroupedSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      preGroupedSymbols.add(Symbol.deserialize(byteBuffer));
    }
    Step step = Step.deserialize(byteBuffer);
    Optional<Symbol> hashSymbol = Optional.empty();
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      hashSymbol = Optional.of(Symbol.deserialize(byteBuffer));
    }
    Optional<Symbol> groupIdSymbol = Optional.empty();
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      groupIdSymbol = Optional.of(Symbol.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AggregationNode(
        planNodeId,
        null,
        aggregations,
        groupingSetDescriptor,
        preGroupedSymbols,
        step,
        hashSymbol,
        groupIdSymbol);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return builderFrom(this).setSource(Iterables.getOnlyElement(newChildren)).build();
  }

  public boolean producesDistinctRows() {
    return aggregations.isEmpty()
        && !groupingSets.getGroupingKeys().isEmpty()
        && outputs.size() == groupingSets.getGroupingKeys().size()
        && outputs.containsAll(new HashSet<>(groupingSets.getGroupingKeys()));
  }

  public boolean isDecomposable(SessionInfo session, Metadata metadata) {
    boolean hasOrderBy =
        getAggregations().values().stream()
            .map(Aggregation::getOrderingScheme)
            .anyMatch(Optional::isPresent);

    boolean hasDistinct = getAggregations().values().stream().anyMatch(Aggregation::isDistinct);

    /*boolean decomposableFunctions = getAggregations().values().stream()
    .map(Aggregation::getResolvedFunction)
    .map(resolvedFunction -> metadata.getAggregationFunctionMetadata(session, resolvedFunction))
    .allMatch(AggregationFunctionMetadata::isDecomposable);*/

    return !hasOrderBy && !hasDistinct;
  }

  public boolean hasSingleNodeExecutionPreference(SessionInfo session, Metadata metadata) {
    // There are two kinds of aggregations the have single node execution preference:
    //
    // 1. aggregations with only empty grouping sets like
    //
    // SELECT count(*) FROM lineitem;
    //
    // there is no need for distributed aggregation. Single node FINAL aggregation will suffice,
    // since all input have to be aggregated into one line output.
    //
    // 2. aggregations that must produce default output and are not decomposable, we cannot
    // distribute them.
    return (hasEmptyGroupingSet() && !hasNonEmptyGroupingSet())
        || (hasDefaultOutput() && !isDecomposable(session, metadata));
  }

  public boolean isStreamable() {
    return !preGroupedSymbols.isEmpty()
        && groupingSets.getGroupingSetCount() == 1
        && groupingSets.getGlobalGroupingSets().isEmpty();
  }

  public static GroupingSetDescriptor globalAggregation() {
    return singleGroupingSet(ImmutableList.of());
  }

  public static GroupingSetDescriptor singleGroupingSet(List<Symbol> groupingKeys) {
    Set<Integer> globalGroupingSets;
    if (groupingKeys.isEmpty()) {
      globalGroupingSets = ImmutableSet.of(0);
    } else {
      globalGroupingSets = ImmutableSet.of();
    }

    return new GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
  }

  public static GroupingSetDescriptor groupingSets(
      List<Symbol> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets) {
    return new GroupingSetDescriptor(groupingKeys, groupingSetCount, globalGroupingSets);
  }

  public static class GroupingSetDescriptor {
    private final List<Symbol> groupingKeys;
    private final int groupingSetCount;
    private final Set<Integer> globalGroupingSets;

    public GroupingSetDescriptor(
        List<Symbol> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets) {
      requireNonNull(globalGroupingSets, "globalGroupingSets is null");
      checkArgument(groupingSetCount > 0, "grouping set count must be larger than 0");
      checkArgument(
          globalGroupingSets.size() <= groupingSetCount,
          "list of empty global grouping sets must be no larger than grouping set count");
      requireNonNull(groupingKeys, "groupingKeys is null");
      if (groupingKeys.isEmpty()) {
        checkArgument(
            !globalGroupingSets.isEmpty(),
            "no grouping keys implies at least one global grouping set, but none provided");
      }

      this.groupingKeys = ImmutableList.copyOf(groupingKeys);
      this.groupingSetCount = groupingSetCount;
      this.globalGroupingSets = ImmutableSet.copyOf(globalGroupingSets);
    }

    public List<Symbol> getGroupingKeys() {
      return groupingKeys;
    }

    public int getGroupingSetCount() {
      return groupingSetCount;
    }

    public Set<Integer> getGlobalGroupingSets() {
      return globalGroupingSets;
    }

    public void serialize(ByteBuffer byteBuffer) {
      ReadWriteIOUtils.write(groupingKeys.size(), byteBuffer);
      for (Symbol symbol : groupingKeys) {
        Symbol.serialize(symbol, byteBuffer);
      }
      ReadWriteIOUtils.write(groupingSetCount, byteBuffer);
      ReadWriteIOUtils.write(globalGroupingSets.size(), byteBuffer);
      for (int globalGroupingSet : globalGroupingSets) {
        ReadWriteIOUtils.write(globalGroupingSet, byteBuffer);
      }
    }

    public void serialize(DataOutputStream stream) throws IOException {
      ReadWriteIOUtils.write(groupingKeys.size(), stream);
      for (Symbol symbol : groupingKeys) {
        Symbol.serialize(symbol, stream);
      }
      ReadWriteIOUtils.write(groupingSetCount, stream);
      ReadWriteIOUtils.write(globalGroupingSets.size(), stream);
      for (int globalGroupingSet : globalGroupingSets) {
        ReadWriteIOUtils.write(globalGroupingSet, stream);
      }
    }

    public static GroupingSetDescriptor deserialize(ByteBuffer byteBuffer) {
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      List<Symbol> groupingKeys = new ArrayList<>(size);
      while (size-- > 0) {
        groupingKeys.add(Symbol.deserialize(byteBuffer));
      }
      int groupingSetCount = ReadWriteIOUtils.readInt(byteBuffer);
      size = ReadWriteIOUtils.readInt(byteBuffer);
      Set<Integer> globalGroupingSets = new HashSet<>(size);
      while (size-- > 0) {
        globalGroupingSets.add(ReadWriteIOUtils.readInt(byteBuffer));
      }
      return new GroupingSetDescriptor(groupingKeys, groupingSetCount, globalGroupingSets);
    }
  }

  public enum Step {
    PARTIAL(true, true),
    FINAL(false, false),
    INTERMEDIATE(false, true),
    SINGLE(true, false);

    private final boolean inputRaw;
    private final boolean outputPartial;

    Step(boolean inputRaw, boolean outputPartial) {
      this.inputRaw = inputRaw;
      this.outputPartial = outputPartial;
    }

    public boolean isInputRaw() {
      return inputRaw;
    }

    public boolean isOutputPartial() {
      return outputPartial;
    }

    public static Step partialOutput(Step step) {
      if (step.isInputRaw()) {
        return Step.PARTIAL;
      }
      return Step.INTERMEDIATE;
    }

    public static Step partialInput(Step step) {
      if (step.isOutputPartial()) {
        return Step.INTERMEDIATE;
      }
      return Step.FINAL;
    }

    public void serialize(ByteBuffer byteBuffer) {
      ReadWriteIOUtils.write(ordinal(), byteBuffer);
    }

    public void serialize(DataOutputStream stream) throws IOException {
      ReadWriteIOUtils.write(ordinal(), stream);
    }

    public static Step deserialize(ByteBuffer byteBuffer) {
      return Step.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    }
  }

  public static class Aggregation {
    private final ResolvedFunction resolvedFunction;
    private final List<Expression> arguments;
    private final boolean distinct;
    private final Optional<Symbol> filter;
    private final Optional<OrderingScheme> orderingScheme;
    private final Optional<Symbol> mask;

    public Aggregation(
        ResolvedFunction resolvedFunction,
        List<Expression> arguments,
        boolean distinct,
        Optional<Symbol> filter,
        Optional<OrderingScheme> orderingScheme,
        Optional<Symbol> mask) {
      this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
      this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
      for (Expression argument : arguments) {
        checkArgument(
            argument instanceof SymbolReference,
            "argument must be symbol: %s",
            argument.getClass().getSimpleName());
      }
      this.distinct = distinct;
      this.filter = requireNonNull(filter, "filter is null");
      this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
      this.mask = requireNonNull(mask, "mask is null");
    }

    public ResolvedFunction getResolvedFunction() {
      return resolvedFunction;
    }

    public List<Expression> getArguments() {
      return arguments;
    }

    public boolean isDistinct() {
      return distinct;
    }

    public Optional<Symbol> getFilter() {
      return filter;
    }

    public Optional<OrderingScheme> getOrderingScheme() {
      return orderingScheme;
    }

    public Optional<Symbol> getMask() {
      return mask;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Aggregation that = (Aggregation) o;
      return distinct == that.distinct
          && Objects.equals(resolvedFunction, that.resolvedFunction)
          && Objects.equals(arguments, that.arguments)
          && Objects.equals(filter, that.filter)
          && Objects.equals(orderingScheme, that.orderingScheme)
          && Objects.equals(mask, that.mask);
    }

    @Override
    public int hashCode() {
      return Objects.hash(resolvedFunction, arguments, distinct, filter, orderingScheme, mask);
    }

    void verifyArguments(Step step) {
      int expectedArgumentCount;
      if (step == Step.SINGLE || step == Step.PARTIAL) {
        expectedArgumentCount = resolvedFunction.getSignature().getArgumentTypes().size();
      } else {
        // Intermediate and final steps get the intermediate value and the lambda functions
        expectedArgumentCount =
            1
                + (int)
                    resolvedFunction.getSignature().getArgumentTypes().stream()
                        .filter(FunctionType.class::isInstance)
                        .count();
      }

      checkArgument(
          expectedArgumentCount == arguments.size(),
          "%s aggregation function %s has %s arguments, but %s arguments were provided to function call",
          step,
          resolvedFunction.getSignature(),
          expectedArgumentCount,
          arguments.size());
    }

    public void serialize(ByteBuffer byteBuffer) {
      resolvedFunction.serialize(byteBuffer);
      ReadWriteIOUtils.write(arguments.size(), byteBuffer);
      for (Expression argument : arguments) {
        Expression.serialize(argument, byteBuffer);
      }
      ReadWriteIOUtils.write(distinct, byteBuffer);
      ReadWriteIOUtils.write(filter.isPresent(), byteBuffer);
      filter.ifPresent(symbol -> Symbol.serialize(symbol, byteBuffer));
      ReadWriteIOUtils.write(orderingScheme.isPresent(), byteBuffer);
      orderingScheme.ifPresent(scheme -> scheme.serialize(byteBuffer));
      ReadWriteIOUtils.write(mask.isPresent(), byteBuffer);
      mask.ifPresent(symbol -> Symbol.serialize(symbol, byteBuffer));
    }

    public void serialize(DataOutputStream stream) throws IOException {
      resolvedFunction.serialize(stream);
      ReadWriteIOUtils.write(arguments.size(), stream);
      for (Expression argument : arguments) {
        Expression.serialize(argument, stream);
      }
      ReadWriteIOUtils.write(distinct, stream);
      ReadWriteIOUtils.write(filter.isPresent(), stream);
      if (filter.isPresent()) {
        Symbol.serialize(filter.get(), stream);
      }
      ReadWriteIOUtils.write(orderingScheme.isPresent(), stream);
      if (orderingScheme.isPresent()) {
        orderingScheme.get().serialize(stream);
      }
      ReadWriteIOUtils.write(mask.isPresent(), stream);
      if (mask.isPresent()) {
        Symbol.serialize(mask.get(), stream);
      }
    }

    public static Aggregation deserialize(ByteBuffer byteBuffer) {
      ResolvedFunction function = ResolvedFunction.deserialize(byteBuffer);
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      List<Expression> arguments = new ArrayList<>(size);
      while (size-- > 0) {
        arguments.add(Expression.deserialize(byteBuffer));
      }
      boolean distinct = ReadWriteIOUtils.readBool(byteBuffer);
      Optional<Symbol> filter = Optional.empty();
      if (ReadWriteIOUtils.readBool(byteBuffer)) {
        filter = Optional.of(Symbol.deserialize(byteBuffer));
      }
      Optional<OrderingScheme> orderingScheme = Optional.empty();
      if (ReadWriteIOUtils.readBool(byteBuffer)) {
        orderingScheme = Optional.of(OrderingScheme.deserialize(byteBuffer));
      }
      Optional<Symbol> mask = Optional.empty();
      if (ReadWriteIOUtils.readBool(byteBuffer)) {
        mask = Optional.of(Symbol.deserialize(byteBuffer));
      }
      return new Aggregation(function, arguments, distinct, filter, orderingScheme, mask);
    }
  }

  public static Builder builderFrom(AggregationNode node) {
    return new Builder(node);
  }

  public static class Builder {
    private PlanNodeId id;
    private PlanNode source;
    private Map<Symbol, Aggregation> aggregations;
    private GroupingSetDescriptor groupingSets;
    private List<Symbol> preGroupedSymbols;
    private Step step;
    private Optional<Symbol> hashSymbol;
    private Optional<Symbol> groupIdSymbol;

    public Builder(AggregationNode node) {
      requireNonNull(node, "node is null");
      this.id = node.getPlanNodeId();
      this.source = node.getChild();
      this.aggregations = node.getAggregations();
      this.groupingSets = node.getGroupingSets();
      this.preGroupedSymbols = node.getPreGroupedSymbols();
      this.step = node.getStep();
      this.hashSymbol = node.getHashSymbol();
      this.groupIdSymbol = node.getGroupIdSymbol();
    }

    public Builder setId(PlanNodeId id) {
      this.id = requireNonNull(id, "id is null");
      return this;
    }

    public Builder setSource(PlanNode source) {
      this.source = requireNonNull(source, "source is null");
      return this;
    }

    public Builder setAggregations(Map<Symbol, Aggregation> aggregations) {
      this.aggregations = requireNonNull(aggregations, "aggregations is null");
      return this;
    }

    public Builder setGroupingSets(GroupingSetDescriptor groupingSets) {
      this.groupingSets = requireNonNull(groupingSets, "groupingSets is null");
      return this;
    }

    public Builder setPreGroupedSymbols(List<Symbol> preGroupedSymbols) {
      this.preGroupedSymbols = requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
      return this;
    }

    public Builder setStep(Step step) {
      this.step = requireNonNull(step, "step is null");
      return this;
    }

    public Builder setHashSymbol(Optional<Symbol> hashSymbol) {
      this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
      return this;
    }

    public Builder setGroupIdSymbol(Optional<Symbol> groupIdSymbol) {
      this.groupIdSymbol = requireNonNull(groupIdSymbol, "groupIdSymbol is null");
      return this;
    }

    public AggregationNode build() {
      return new AggregationNode(
          id,
          source,
          aggregations,
          groupingSets,
          preGroupedSymbols,
          step,
          hashSymbol,
          groupIdSymbol);
    }
  }
}
