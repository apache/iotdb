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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableFunctionNode extends MultiChildProcessNode {

  private final String name;
  private final Map<String, Argument> arguments;
  private final List<Symbol> properOutputs;
  private final List<TableArgumentProperties> tableArgumentProperties;

  public TableFunctionNode(
      PlanNodeId id,
      String name,
      Map<String, Argument> arguments,
      List<Symbol> properOutputs,
      List<PlanNode> children,
      List<TableArgumentProperties> tableArgumentProperties) {
    super(id, children);
    this.name = requireNonNull(name, "name is null");
    this.arguments = ImmutableMap.copyOf(arguments);
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.tableArgumentProperties = ImmutableList.copyOf(tableArgumentProperties);
  }

  public TableFunctionNode(
      PlanNodeId id,
      String name,
      Map<String, Argument> arguments,
      List<Symbol> properOutputs,
      List<TableArgumentProperties> tableArgumentProperties) {
    super(id);
    this.name = requireNonNull(name, "name is null");
    this.arguments = ImmutableMap.copyOf(arguments);
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.tableArgumentProperties = ImmutableList.copyOf(tableArgumentProperties);
  }

  public String getName() {
    return name;
  }

  public Map<String, Argument> getArguments() {
    return arguments;
  }

  public List<Symbol> getProperOutputs() {
    return properOutputs;
  }

  public List<TableArgumentProperties> getTableArgumentProperties() {
    return tableArgumentProperties;
  }

  @Override
  public PlanNode clone() {
    return new TableFunctionNode(id, name, arguments, properOutputs, tableArgumentProperties);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
    symbols.addAll(properOutputs);
    tableArgumentProperties.stream()
        .map(TableArgumentProperties::getPassThroughSpecification)
        .map(PassThroughSpecification::getColumns)
        .flatMap(Collection::stream)
        .map(PassThroughColumn::getSymbol)
        .forEach(symbols::add);
    return symbols.build();
  }

  @Override
  public List<String> getOutputColumnNames() {
    ImmutableList.Builder<String> symbols = ImmutableList.builder();
    symbols.addAll(properOutputs.stream().map(Symbol::getName).collect(Collectors.toList()));
    tableArgumentProperties.stream()
        .map(TableArgumentProperties::getPassThroughSpecification)
        .map(PassThroughSpecification::getColumns)
        .flatMap(Collection::stream)
        .map(PassThroughColumn::getSymbol)
        .map(Symbol::getName)
        .forEach(symbols::add);
    return symbols.build();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newSources) {
    checkArgument(children.size() == newSources.size(), "wrong number of new children");
    return new TableFunctionNode(
        getPlanNodeId(), name, arguments, properOutputs, newSources, tableArgumentProperties);
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
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableFunction(this, context);
  }

  public static class TableArgumentProperties {
    private final String argumentName;
    private final boolean rowSemantics;
    private final boolean pruneWhenEmpty;
    private final PassThroughSpecification passThroughSpecification;
    private final List<Symbol> requiredColumns;
    private final Optional<DataOrganizationSpecification> dataOrganizationSpecification;

    public TableArgumentProperties(
        String argumentName,
        boolean rowSemantics,
        boolean pruneWhenEmpty,
        PassThroughSpecification passThroughSpecification,
        List<Symbol> requiredColumns,
        Optional<DataOrganizationSpecification> dataOrganizationSpecification) {
      this.argumentName = requireNonNull(argumentName, "argumentName is null");
      this.rowSemantics = rowSemantics;
      this.pruneWhenEmpty = pruneWhenEmpty;
      this.passThroughSpecification =
          requireNonNull(passThroughSpecification, "passThroughSpecification is null");
      this.requiredColumns = ImmutableList.copyOf(requiredColumns);
      this.dataOrganizationSpecification =
          requireNonNull(dataOrganizationSpecification, "specification is null");
    }

    public String getArgumentName() {
      return argumentName;
    }

    public List<Symbol> getRequiredColumns() {
      return requiredColumns;
    }

    public boolean isRowSemantics() {
      return rowSemantics;
    }

    public boolean isPruneWhenEmpty() {
      return pruneWhenEmpty;
    }

    public PassThroughSpecification getPassThroughSpecification() {
      return passThroughSpecification;
    }

    public Optional<DataOrganizationSpecification> getDataOrganizationSpecification() {
      return dataOrganizationSpecification;
    }
  }

  public static class PassThroughSpecification {
    private final boolean declaredAsPassThrough;
    private final List<PassThroughColumn> columns;

    public PassThroughSpecification(
        boolean declaredAsPassThrough, List<PassThroughColumn> columns) {

      if (!declaredAsPassThrough
          && !columns.stream().allMatch(PassThroughColumn::isPartitioningColumn)) {
        throw new IllegalArgumentException(
            "non-partitioning pass-through column for non-pass-through source of a table function");
      }
      this.columns = ImmutableList.copyOf(columns);
      this.declaredAsPassThrough = declaredAsPassThrough;
    }

    public boolean isDeclaredAsPassThrough() {
      return declaredAsPassThrough;
    }

    public List<PassThroughColumn> getColumns() {
      return columns;
    }
  }

  public static class PassThroughColumn {
    private final Symbol symbol;
    private final boolean isPartitioningColumn;

    public PassThroughColumn(Symbol symbol, boolean isPartitioningColumn) {
      this.symbol = requireNonNull(symbol, "symbol is null");
      this.isPartitioningColumn = isPartitioningColumn;
    }

    public Symbol getSymbol() {
      return symbol;
    }

    public boolean isPartitioningColumn() {
      return isPartitioningColumn;
    }
  }
}
