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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableFunctionNode extends MultiChildProcessNode {

  private final String name;
  private final TableFunctionHandle tableFunctionHandle;
  private final List<Symbol> properOutputs;
  private final List<TableArgumentProperties> tableArgumentProperties;

  public TableFunctionNode(
      PlanNodeId id,
      String name,
      TableFunctionHandle tableFunctionHandle,
      List<Symbol> properOutputs,
      List<PlanNode> children,
      List<TableArgumentProperties> tableArgumentProperties) {
    super(id, children);
    this.name = requireNonNull(name, "name is null");
    this.tableFunctionHandle = tableFunctionHandle;
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.tableArgumentProperties = ImmutableList.copyOf(tableArgumentProperties);
  }

  public TableFunctionNode(
      PlanNodeId id,
      String name,
      TableFunctionHandle tableFunctionHandle,
      List<Symbol> properOutputs,
      List<TableArgumentProperties> tableArgumentProperties) {
    super(id);
    this.name = requireNonNull(name, "name is null");
    this.tableFunctionHandle = tableFunctionHandle;
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.tableArgumentProperties = ImmutableList.copyOf(tableArgumentProperties);
  }

  public String getName() {
    return name;
  }

  public TableFunctionHandle getTableFunctionHandle() {
    return tableFunctionHandle;
  }

  public List<Symbol> getProperOutputs() {
    return properOutputs;
  }

  public List<TableArgumentProperties> getTableArgumentProperties() {
    return tableArgumentProperties;
  }

  @Override
  public PlanNode clone() {
    return new TableFunctionNode(
        id, name, tableFunctionHandle, properOutputs, tableArgumentProperties);
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
        getPlanNodeId(),
        name,
        tableFunctionHandle,
        properOutputs,
        newSources,
        tableArgumentProperties);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_FUNCTION_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(name, byteBuffer);
    byte[] bytes = tableFunctionHandle.serialize();
    ReadWriteIOUtils.write(bytes.length, byteBuffer);
    ReadWriteIOUtils.write(ByteBuffer.wrap(bytes), byteBuffer);
    ReadWriteIOUtils.write(properOutputs.size(), byteBuffer);
    properOutputs.forEach(symbol -> Symbol.serialize(symbol, byteBuffer));
    ReadWriteIOUtils.write(tableArgumentProperties.size(), byteBuffer);
    tableArgumentProperties.forEach(
        properties -> {
          ReadWriteIOUtils.write(properties.getArgumentName(), byteBuffer);
          ReadWriteIOUtils.write(properties.isRowSemantics(), byteBuffer);
          properties.getPassThroughSpecification().serialize(byteBuffer);
          ReadWriteIOUtils.write(properties.getRequiredColumns().size(), byteBuffer);
          properties.getRequiredColumns().forEach(symbol -> Symbol.serialize(symbol, byteBuffer));
          ReadWriteIOUtils.write(
              properties.getDataOrganizationSpecification().isPresent(), byteBuffer);
          properties
              .getDataOrganizationSpecification()
              .ifPresent(specification -> specification.serialize(byteBuffer));
          ReadWriteIOUtils.write(properties.isRequireRecordSnapshot(), byteBuffer);
        });
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_FUNCTION_NODE.serialize(stream);
    ReadWriteIOUtils.write(name, stream);
    byte[] bytes = tableFunctionHandle.serialize();
    ReadWriteIOUtils.write(bytes.length, stream);
    ReadWriteIOUtils.write(ByteBuffer.wrap(bytes), stream);
    ReadWriteIOUtils.write(properOutputs.size(), stream);
    for (Symbol symbol : properOutputs) {
      Symbol.serialize(symbol, stream);
    }
    ReadWriteIOUtils.write(tableArgumentProperties.size(), stream);
    for (TableArgumentProperties properties : tableArgumentProperties) {
      ReadWriteIOUtils.write(properties.getArgumentName(), stream);
      ReadWriteIOUtils.write(properties.isRowSemantics(), stream);
      properties.getPassThroughSpecification().serialize(stream);
      ReadWriteIOUtils.write(properties.getRequiredColumns().size(), stream);
      for (Symbol symbol : properties.getRequiredColumns()) {
        Symbol.serialize(symbol, stream);
      }
      ReadWriteIOUtils.write(properties.getDataOrganizationSpecification().isPresent(), stream);
      if (properties.getDataOrganizationSpecification().isPresent()) {
        properties.getDataOrganizationSpecification().get().serialize(stream);
      }
      ReadWriteIOUtils.write(properties.isRequireRecordSnapshot(), stream);
    }
  }

  public static TableFunctionNode deserialize(ByteBuffer byteBuffer) {
    String name = ReadWriteIOUtils.readString(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    byte[] bytes = ReadWriteIOUtils.readBytes(byteBuffer, size);
    TableFunctionHandle tableFunctionHandle =
        new TableMetadataImpl().getTableFunction(name).createTableFunctionHandle();
    tableFunctionHandle.deserialize(bytes);
    size = ReadWriteIOUtils.readInt(byteBuffer);
    ImmutableList.Builder<Symbol> properOutputs = ImmutableList.builder();
    for (int i = 0; i < size; i++) {
      properOutputs.add(Symbol.deserialize(byteBuffer));
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    ImmutableList.Builder<TableArgumentProperties> tableArgumentProperties =
        ImmutableList.builder();
    for (int i = 0; i < size; i++) {
      String argumentName = ReadWriteIOUtils.readString(byteBuffer);
      boolean rowSemantics = ReadWriteIOUtils.readBoolean(byteBuffer);
      PassThroughSpecification passThroughSpecification =
          PassThroughSpecification.deserialize(byteBuffer);
      int requiredColumnsSize = ReadWriteIOUtils.readInt(byteBuffer);
      ImmutableList.Builder<Symbol> requiredColumns = ImmutableList.builder();
      for (int j = 0; j < requiredColumnsSize; j++) {
        requiredColumns.add(Symbol.deserialize(byteBuffer));
      }
      Optional<DataOrganizationSpecification> dataOrganizationSpecification = Optional.empty();
      if (ReadWriteIOUtils.readBoolean(byteBuffer)) {
        dataOrganizationSpecification =
            Optional.of(DataOrganizationSpecification.deserialize(byteBuffer));
      }
      boolean requireRecordSnapshot = ReadWriteIOUtils.readBoolean(byteBuffer);
      tableArgumentProperties.add(
          new TableArgumentProperties(
              argumentName,
              rowSemantics,
              passThroughSpecification,
              requiredColumns.build(),
              dataOrganizationSpecification,
              requireRecordSnapshot));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new TableFunctionNode(
        planNodeId,
        name,
        tableFunctionHandle,
        properOutputs.build(),
        tableArgumentProperties.build());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableFunction(this, context);
  }

  public static class TableArgumentProperties {
    private final String argumentName;
    private final boolean rowSemantics;
    private final PassThroughSpecification passThroughSpecification;
    private final List<Symbol> requiredColumns;
    private final Optional<DataOrganizationSpecification> dataOrganizationSpecification;
    private final boolean requireRecordSnapshot;

    public TableArgumentProperties(
        String argumentName,
        boolean rowSemantics,
        PassThroughSpecification passThroughSpecification,
        List<Symbol> requiredColumns,
        Optional<DataOrganizationSpecification> dataOrganizationSpecification,
        boolean requireRecordSnapshot) {
      this.argumentName = requireNonNull(argumentName, "argumentName is null");
      this.rowSemantics = rowSemantics;
      this.passThroughSpecification =
          requireNonNull(passThroughSpecification, "passThroughSpecification is null");
      this.requiredColumns = ImmutableList.copyOf(requiredColumns);
      this.dataOrganizationSpecification =
          requireNonNull(dataOrganizationSpecification, "specification is null");
      this.requireRecordSnapshot = requireRecordSnapshot;
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

    public PassThroughSpecification getPassThroughSpecification() {
      return passThroughSpecification;
    }

    public Optional<DataOrganizationSpecification> getDataOrganizationSpecification() {
      return dataOrganizationSpecification;
    }

    public boolean isRequireRecordSnapshot() {
      return requireRecordSnapshot;
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

    public void serialize(DataOutputStream stream) throws IOException {
      ReadWriteIOUtils.write(declaredAsPassThrough, stream);
      ReadWriteIOUtils.write(columns.size(), stream);
      for (PassThroughColumn column : columns) {
        ReadWriteIOUtils.write(column.isPartitioningColumn, stream);
        Symbol.serialize(column.getSymbol(), stream);
      }
    }

    public void serialize(ByteBuffer buffer) {
      ReadWriteIOUtils.write(declaredAsPassThrough, buffer);
      ReadWriteIOUtils.write(columns.size(), buffer);
      for (PassThroughColumn column : columns) {
        ReadWriteIOUtils.write(column.isPartitioningColumn, buffer);
        Symbol.serialize(column.getSymbol(), buffer);
      }
    }

    public static PassThroughSpecification deserialize(ByteBuffer byteBuffer) {
      boolean declaredAsPassThrough = ReadWriteIOUtils.readBoolean(byteBuffer);
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      ImmutableList.Builder<PassThroughColumn> columns = ImmutableList.builder();
      for (int i = 0; i < size; i++) {
        boolean isPartitioningColumn = ReadWriteIOUtils.readBoolean(byteBuffer);
        Symbol symbol = Symbol.deserialize(byteBuffer);
        columns.add(new PassThroughColumn(symbol, isPartitioningColumn));
      }
      return new PassThroughSpecification(declaredAsPassThrough, columns.build());
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
