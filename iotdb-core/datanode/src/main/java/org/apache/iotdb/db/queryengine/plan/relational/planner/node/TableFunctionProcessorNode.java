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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class TableFunctionProcessorNode extends SingleChildProcessNode {

  private final String name;

  // symbols produced by the function
  private final List<Symbol> properOutputs;

  // all source symbols to be produced on output, ordered as table argument specifications
  private final Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification;

  // symbols required from each source, ordered as table argument specifications
  private final List<Symbol> requiredSymbols;

  // partitioning and ordering combined from sources
  private final Optional<DataOrganizationSpecification> dataOrganizationSpecification;

  private final boolean rowSemantic;

  private final TableFunctionHandle tableFunctionHandle;

  private final boolean requireRecordSnapshot;

  public TableFunctionProcessorNode(
      PlanNodeId id,
      String name,
      List<Symbol> properOutputs,
      Optional<PlanNode> source,
      Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification,
      List<Symbol> requiredSymbols,
      Optional<DataOrganizationSpecification> dataOrganizationSpecification,
      boolean rowSemantic,
      TableFunctionHandle tableFunctionHandle,
      boolean requireRecordSnapshot) {
    super(id, source.orElse(null));
    this.name = requireNonNull(name, "name is null");
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.passThroughSpecification = passThroughSpecification;
    this.requiredSymbols = ImmutableList.copyOf(requiredSymbols);
    this.dataOrganizationSpecification =
        requireNonNull(dataOrganizationSpecification, "specification is null");
    this.rowSemantic = rowSemantic;
    this.tableFunctionHandle = tableFunctionHandle;
    this.requireRecordSnapshot = requireRecordSnapshot;
  }

  public TableFunctionProcessorNode(
      PlanNodeId id,
      String name,
      List<Symbol> properOutputs,
      Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification,
      List<Symbol> requiredSymbols,
      Optional<DataOrganizationSpecification> dataOrganizationSpecification,
      boolean rowSemantic,
      TableFunctionHandle tableFunctionHandle,
      boolean requireRecordSnapshot) {
    super(id);
    this.name = requireNonNull(name, "name is null");
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.passThroughSpecification = passThroughSpecification;
    this.requiredSymbols = ImmutableList.copyOf(requiredSymbols);
    this.dataOrganizationSpecification =
        requireNonNull(dataOrganizationSpecification, "specification is null");
    this.rowSemantic = rowSemantic;
    this.tableFunctionHandle = tableFunctionHandle;
    this.requireRecordSnapshot = requireRecordSnapshot;
  }

  public String getName() {
    return name;
  }

  public List<Symbol> getProperOutputs() {
    return properOutputs;
  }

  public boolean isRowSemantic() {
    return rowSemantic;
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

  public TableFunctionHandle getTableFunctionHandle() {
    return tableFunctionHandle;
  }

  public boolean isRequireRecordSnapshot() {
    return requireRecordSnapshot;
  }

  @Override
  public PlanNode clone() {
    return new TableFunctionProcessorNode(
        id,
        name,
        properOutputs,
        passThroughSpecification,
        requiredSymbols,
        dataOrganizationSpecification,
        rowSemantic,
        tableFunctionHandle,
        requireRecordSnapshot);
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
    PlanNodeType.TABLE_FUNCTION_PROCESSOR_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(name, byteBuffer);
    ReadWriteIOUtils.write(properOutputs.size(), byteBuffer);
    properOutputs.forEach(symbol -> Symbol.serialize(symbol, byteBuffer));
    ReadWriteIOUtils.write(passThroughSpecification.isPresent(), byteBuffer);
    passThroughSpecification.ifPresent(
        passThroughSpecification1 -> passThroughSpecification1.serialize(byteBuffer));
    ReadWriteIOUtils.write(requiredSymbols.size(), byteBuffer);
    requiredSymbols.forEach(symbol -> Symbol.serialize(symbol, byteBuffer));
    ReadWriteIOUtils.write(dataOrganizationSpecification.isPresent(), byteBuffer);
    if (dataOrganizationSpecification.isPresent()) {
      dataOrganizationSpecification.get().serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(rowSemantic, byteBuffer);
    byte[] bytes = tableFunctionHandle.serialize();
    ReadWriteIOUtils.write(ByteBuffer.wrap(bytes), byteBuffer);
    ReadWriteIOUtils.write(requireRecordSnapshot, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_FUNCTION_PROCESSOR_NODE.serialize(stream);
    ReadWriteIOUtils.write(name, stream);
    ReadWriteIOUtils.write(properOutputs.size(), stream);
    for (Symbol symbol : properOutputs) {
      Symbol.serialize(symbol, stream);
    }
    ReadWriteIOUtils.write(passThroughSpecification.isPresent(), stream);
    if (passThroughSpecification.isPresent()) {
      passThroughSpecification.get().serialize(stream);
    }
    ReadWriteIOUtils.write(requiredSymbols.size(), stream);
    for (Symbol symbol : requiredSymbols) {
      Symbol.serialize(symbol, stream);
    }
    ReadWriteIOUtils.write(dataOrganizationSpecification.isPresent(), stream);
    if (dataOrganizationSpecification.isPresent()) {
      dataOrganizationSpecification.get().serialize(stream);
    }
    ReadWriteIOUtils.write(rowSemantic, stream);
    byte[] bytes = tableFunctionHandle.serialize();
    ReadWriteIOUtils.write(ByteBuffer.wrap(bytes), stream);
    ReadWriteIOUtils.write(requireRecordSnapshot, stream);
  }

  public static TableFunctionProcessorNode deserialize(ByteBuffer byteBuffer) {
    String name = ReadWriteIOUtils.readString(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> properOutputs = new ArrayList<>(size);
    while (size-- > 0) {
      properOutputs.add(Symbol.deserialize(byteBuffer));
    }
    boolean hasPassThroughSpecification = ReadWriteIOUtils.readBoolean(byteBuffer);
    Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification =
        hasPassThroughSpecification
            ? Optional.of(TableFunctionNode.PassThroughSpecification.deserialize(byteBuffer))
            : Optional.empty();
    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> requiredSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      requiredSymbols.add(Symbol.deserialize(byteBuffer));
    }
    boolean hasDataOrganizationSpecification = ReadWriteIOUtils.readBoolean(byteBuffer);
    Optional<DataOrganizationSpecification> dataOrganizationSpecification =
        hasDataOrganizationSpecification
            ? Optional.of(DataOrganizationSpecification.deserialize(byteBuffer))
            : Optional.empty();
    boolean rowSemantic = ReadWriteIOUtils.readBoolean(byteBuffer);
    size = ReadWriteIOUtils.readInt(byteBuffer);
    byte[] bytes = ReadWriteIOUtils.readBytes(byteBuffer, size);
    TableFunctionHandle tableFunctionHandle =
        new TableMetadataImpl().getTableFunction(name).createTableFunctionHandle();
    tableFunctionHandle.deserialize(bytes);
    boolean requireRecordSnapshot = ReadWriteIOUtils.readBoolean(byteBuffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new TableFunctionProcessorNode(
        planNodeId,
        name,
        properOutputs,
        passThroughSpecification,
        requiredSymbols,
        dataOrganizationSpecification,
        rowSemantic,
        tableFunctionHandle,
        requireRecordSnapshot);
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
        passThroughSpecification,
        requiredSymbols,
        dataOrganizationSpecification,
        rowSemantic,
        tableFunctionHandle,
        requireRecordSnapshot);
  }
}
