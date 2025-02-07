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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class TableFunctionProcessorNode extends SingleChildProcessNode {

  private final String name;

  // symbols produced by the function
  private final List<Symbol> properOutputs;

  // specifies whether the function should be pruned or executed when the input is empty
  // pruneWhenEmpty is false if and only if all original input tables are KEEP WHEN EMPTY
  private final boolean pruneWhenEmpty;

  // all source symbols to be produced on output, ordered as table argument specifications
  private final Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification;

  // symbols required from each source, ordered as table argument specifications
  private final List<Symbol> requiredSymbols;

  // partitioning and ordering combined from sources
  private final Optional<DataOrganizationSpecification> dataOrganizationSpecification;

  private final Map<String, Argument> arguments;

  public TableFunctionProcessorNode(
      PlanNodeId id,
      String name,
      List<Symbol> properOutputs,
      Optional<PlanNode> source,
      boolean pruneWhenEmpty,
      Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification,
      List<Symbol> requiredSymbols,
      Optional<DataOrganizationSpecification> dataOrganizationSpecification,
      Map<String, Argument> arguments) {
    super(id, source.orElse(null));
    this.name = requireNonNull(name, "name is null");
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.pruneWhenEmpty = pruneWhenEmpty;
    this.passThroughSpecification = passThroughSpecification;
    this.requiredSymbols = ImmutableList.copyOf(requiredSymbols);
    this.dataOrganizationSpecification =
        requireNonNull(dataOrganizationSpecification, "specification is null");
    this.arguments = ImmutableMap.copyOf(arguments);
  }

  public TableFunctionProcessorNode(
      PlanNodeId id,
      String name,
      List<Symbol> properOutputs,
      boolean pruneWhenEmpty,
      Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification,
      List<Symbol> requiredSymbols,
      Optional<DataOrganizationSpecification> dataOrganizationSpecification,
      Map<String, Argument> arguments) {
    super(id);
    this.name = requireNonNull(name, "name is null");
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.pruneWhenEmpty = pruneWhenEmpty;
    this.passThroughSpecification = passThroughSpecification;
    this.requiredSymbols = ImmutableList.copyOf(requiredSymbols);
    this.dataOrganizationSpecification =
        requireNonNull(dataOrganizationSpecification, "specification is null");
    this.arguments = ImmutableMap.copyOf(arguments);
  }

  public String getName() {
    return name;
  }

  public List<Symbol> getProperOutputs() {
    return properOutputs;
  }

  public boolean isPruneWhenEmpty() {
    return pruneWhenEmpty;
  }

  public Optional<TableFunctionNode.PassThroughSpecification> getPassThroughSpecification() {
    return passThroughSpecification;
  }

  public List<Symbol> getRequiredSymbols() {
    return requiredSymbols;
  }

  public Optional<DataOrganizationSpecification> getDataOrganizationSpecification() {
    return dataOrganizationSpecification;
  }

  public Map<String, Argument> getArguments() {
    return arguments;
  }

  @Override
  public PlanNode clone() {
    return new TableFunctionProcessorNode(
        id,
        name,
        properOutputs,
        pruneWhenEmpty,
        passThroughSpecification,
        requiredSymbols,
        dataOrganizationSpecification,
        arguments);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
    symbols.addAll(properOutputs);
    passThroughSpecification.ifPresent(
        passThroughSpecification1 ->
            passThroughSpecification1.getColumns().stream()
                .map(TableFunctionNode.PassThroughColumn::getSymbol)
                .forEach(symbols::add));
    return symbols.build();
  }

  @Override
  public List<String> getOutputColumnNames() {
    ImmutableList.Builder<String> symbols = ImmutableList.builder();
    symbols.addAll(properOutputs.stream().map(Symbol::getName).collect(Collectors.toList()));
    passThroughSpecification.ifPresent(
        passThroughSpecification1 ->
            passThroughSpecification1.getColumns().stream()
                .map(TableFunctionNode.PassThroughColumn::getSymbol)
                .map(Symbol::getName)
                .forEach(symbols::add));
    return symbols.build();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableFunctionProcessor(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    // TODO(UDF)
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    // TODO(UDF)
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newSources) {
    Optional<PlanNode> newSource =
        newSources.isEmpty() ? Optional.empty() : Optional.of(getOnlyElement(newSources));
    return new TableFunctionProcessorNode(
        id,
        name,
        properOutputs,
        newSource,
        pruneWhenEmpty,
        passThroughSpecification,
        requiredSymbols,
        dataOrganizationSpecification,
        arguments);
  }
}
