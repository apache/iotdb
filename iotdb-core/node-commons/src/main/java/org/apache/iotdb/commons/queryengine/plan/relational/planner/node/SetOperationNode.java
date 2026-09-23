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

package org.apache.iotdb.commons.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public abstract class SetOperationNode extends MultiChildProcessNode {
  private final ListMultimap<Symbol, Symbol> outputToInputs;
  private final List<Symbol> outputs;

  protected SetOperationNode(
      PlanNodeId id,
      List<PlanNode> children,
      ListMultimap<Symbol, Symbol> outputToInputs,
      List<Symbol> outputs) {
    super(id);

    requireNonNull(children, QueryMessages.EXCEPTION_CHILDREN_IS_NULL_0CB43CE2);
    checkArgument(
        !children.isEmpty(), QueryMessages.EXCEPTION_MUST_HAVE_AT_LEAST_ONE_SOURCE_4114F454);
    requireNonNull(outputToInputs, QueryMessages.EXCEPTION_OUTPUTTOINPUTS_IS_NULL_4125312B);
    requireNonNull(outputs, QueryMessages.EXCEPTION_OUTPUTS_IS_NULL_EFD078A2);

    this.children = ImmutableList.copyOf(children);
    this.outputToInputs = ImmutableListMultimap.copyOf(outputToInputs);
    this.outputs = ImmutableList.copyOf(outputs);

    for (Collection<Symbol> inputs : this.outputToInputs.asMap().values()) {
      checkArgument(
          inputs.size() == this.children.size(),
          QueryMessages
              .EXCEPTION_EVERY_CHILD_NEEDS_TO_MAP_ITS_SYMBOLS_TO_AN_OUTPUT_ARG_OPERATION_SYMBOL_7FA107A2,
          this.getClass().getSimpleName());
    }

    // Make sure each child positionally corresponds to their Symbol values in the Multimap
    for (int i = 0; i < children.size(); i++) {
      Set<Symbol> childSymbols = ImmutableSet.copyOf(children.get(i).getOutputSymbols());
      for (Collection<Symbol> expectedInputs : this.outputToInputs.asMap().values()) {
        checkArgument(
            childSymbols.contains(Iterables.get(expectedInputs, i)),
            QueryMessages.EXCEPTION_CHILD_DOES_NOT_PROVIDE_REQUIRED_SYMBOLS_B7CE956E);
      }
    }
  }

  // used for clone(), we needn't check arguments again
  protected SetOperationNode(
      PlanNodeId id, ListMultimap<Symbol, Symbol> outputToInputs, List<Symbol> outputs) {
    super(id);

    this.outputToInputs = ImmutableListMultimap.copyOf(outputToInputs);
    this.outputs = ImmutableList.copyOf(outputs);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputs;
  }

  public ListMultimap<Symbol, Symbol> getSymbolMapping() {
    return outputToInputs;
  }

  public List<Symbol> sourceOutputLayout(int sourceIndex) {
    // Make sure the sourceOutputLayout symbols are listed in the same order as the corresponding
    // output symbols
    return getOutputSymbols().stream()
        .map(symbol -> outputToInputs.get(symbol).get(sourceIndex))
        .collect(toImmutableList());
  }

  /** Returns the output to input symbol mapping for the given source channel */
  public Map<Symbol, SymbolReference> sourceSymbolMap(int sourceIndex) {
    ImmutableMap.Builder<Symbol, SymbolReference> builder = ImmutableMap.builder();
    for (Map.Entry<Symbol, Collection<Symbol>> entry : outputToInputs.asMap().entrySet()) {
      builder.put(entry.getKey(), Iterables.get(entry.getValue(), sourceIndex).toSymbolReference());
    }

    return builder.buildOrThrow();
  }

  /**
   * Returns the input to output symbol mapping for the given source channel. A single input symbol
   * can map to multiple output symbols, thus requiring a Multimap.
   */
  public Multimap<Symbol, SymbolReference> outputSymbolMap(int sourceIndex) {
    return Multimaps.transformValues(
        FluentIterable.from(getOutputSymbols())
            .toMap(outputToSourceSymbolFunction(sourceIndex))
            .asMultimap()
            .inverse(),
        Symbol::toSymbolReference);
  }

  private Function<Symbol, Symbol> outputToSourceSymbolFunction(int sourceIndex) {
    return outputSymbol -> outputToInputs.get(outputSymbol).get(sourceIndex);
  }
}
