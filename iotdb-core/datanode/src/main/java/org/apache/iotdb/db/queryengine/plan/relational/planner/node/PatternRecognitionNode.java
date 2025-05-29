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
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrRowPattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowsPerMatch.ONE;

public class PatternRecognitionNode extends SingleChildProcessNode {
  private final List<Symbol> partitionBy;
  private final Optional<OrderingScheme> orderingScheme;
  private final Optional<Symbol> hashSymbol;
  private final Map<Symbol, Measure> measures;
  private final RowsPerMatch rowsPerMatch;
  private final Set<IrLabel> skipToLabels;
  private final SkipToPosition skipToPosition;
  private final IrRowPattern pattern;
  private final Map<IrLabel, ExpressionAndValuePointers> variableDefinitions;

  public PatternRecognitionNode(
      PlanNodeId id,
      PlanNode child,
      List<Symbol> partitionBy,
      Optional<OrderingScheme> orderingScheme,
      Optional<Symbol> hashSymbol,
      Map<Symbol, Measure> measures,
      RowsPerMatch rowsPerMatch,
      Set<IrLabel> skipToLabels,
      SkipToPosition skipToPosition,
      IrRowPattern pattern,
      Map<IrLabel, ExpressionAndValuePointers> variableDefinitions) {
    super(id, child);

    this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
    this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
    this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
    this.measures = requireNonNull(measures, "measures is null");
    this.rowsPerMatch = requireNonNull(rowsPerMatch, "rowsPerMatch is null");
    this.skipToLabels = requireNonNull(skipToLabels, "skipToLabels is null");
    this.skipToPosition = requireNonNull(skipToPosition, "skipToPosition is null");
    this.pattern = requireNonNull(pattern, "pattern is null");
    this.variableDefinitions = requireNonNull(variableDefinitions, "variableDefinitions is null");
  }

  @Override
  // The order of symbols in the returned list might be different than expected layout of the node
  public List<Symbol> getOutputSymbols() {
    ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
    if (rowsPerMatch == ONE) {
      outputSymbols.addAll(partitionBy);
    } else {
      outputSymbols.addAll(child.getOutputSymbols());
    }
    outputSymbols.addAll(measures.keySet());

    return outputSymbols.build();
  }

  public Set<Symbol> getCreatedSymbols() {
    return ImmutableSet.copyOf(measures.keySet());
  }

  public List<Symbol> getPartitionBy() {
    return partitionBy;
  }

  public Optional<OrderingScheme> getOrderingScheme() {
    return orderingScheme;
  }

  public Optional<Symbol> getHashSymbol() {
    return hashSymbol;
  }

  public Map<Symbol, Measure> getMeasures() {
    return measures;
  }

  public RowsPerMatch getRowsPerMatch() {
    return rowsPerMatch;
  }

  public Set<IrLabel> getSkipToLabels() {
    return skipToLabels;
  }

  public SkipToPosition getSkipToPosition() {
    return skipToPosition;
  }

  public IrRowPattern getPattern() {
    return pattern;
  }

  public Map<IrLabel, ExpressionAndValuePointers> getVariableDefinitions() {
    return variableDefinitions;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPatternRecognition(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new PatternRecognitionNode(
        id,
        Iterables.getOnlyElement(newChildren),
        partitionBy,
        orderingScheme,
        hashSymbol,
        measures,
        rowsPerMatch,
        skipToLabels,
        skipToPosition,
        pattern,
        variableDefinitions);
  }

  @Override
  public PlanNode clone() {
    return new PatternRecognitionNode(
        id,
        null,
        partitionBy,
        orderingScheme,
        hashSymbol,
        measures,
        rowsPerMatch,
        skipToLabels,
        skipToPosition,
        pattern,
        variableDefinitions);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_PATTERN_RECOGNITION_NODE.serialize(byteBuffer);

    ReadWriteIOUtils.write(partitionBy.size(), byteBuffer);
    for (Symbol symbol : partitionBy) {
      Symbol.serialize(symbol, byteBuffer);
    }

    if (orderingScheme.isPresent()) {
      ReadWriteIOUtils.write(true, byteBuffer);
      orderingScheme.get().serialize(byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    if (hashSymbol.isPresent()) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Symbol.serialize(hashSymbol.get(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    ReadWriteIOUtils.write(measures.size(), byteBuffer);
    for (Map.Entry<Symbol, Measure> entry : measures.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      Measure.serialize(entry.getValue(), byteBuffer);
    }

    RowsPerMatch.serialize(rowsPerMatch, byteBuffer);

    ReadWriteIOUtils.write(skipToLabels.size(), byteBuffer);
    for (IrLabel label : skipToLabels) {
      IrLabel.serialize(label, byteBuffer);
    }

    SkipToPosition.serialize(skipToPosition, byteBuffer);

    IrRowPattern.serialize(pattern, byteBuffer);

    ReadWriteIOUtils.write(variableDefinitions.size(), byteBuffer);
    for (Map.Entry<IrLabel, ExpressionAndValuePointers> entry : variableDefinitions.entrySet()) {
      IrLabel.serialize(entry.getKey(), byteBuffer);
      ExpressionAndValuePointers.serialize(entry.getValue(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_PATTERN_RECOGNITION_NODE.serialize(stream);

    ReadWriteIOUtils.write(partitionBy.size(), stream);
    for (Symbol symbol : partitionBy) {
      Symbol.serialize(symbol, stream);
    }

    if (orderingScheme.isPresent()) {
      ReadWriteIOUtils.write(true, stream);
      orderingScheme.get().serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    if (hashSymbol.isPresent()) {
      ReadWriteIOUtils.write(true, stream);
      Symbol.serialize(hashSymbol.get(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    ReadWriteIOUtils.write(measures.size(), stream);
    for (Map.Entry<Symbol, Measure> entry : measures.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      Measure.serialize(entry.getValue(), stream);
    }

    RowsPerMatch.serialize(rowsPerMatch, stream);

    ReadWriteIOUtils.write(skipToLabels.size(), stream);
    for (IrLabel label : skipToLabels) {
      IrLabel.serialize(label, stream);
    }

    SkipToPosition.serialize(skipToPosition, stream);

    IrRowPattern.serialize(pattern, stream);

    ReadWriteIOUtils.write(variableDefinitions.size(), stream);
    for (Map.Entry<IrLabel, ExpressionAndValuePointers> entry : variableDefinitions.entrySet()) {
      IrLabel.serialize(entry.getKey(), stream);
      ExpressionAndValuePointers.serialize(entry.getValue(), stream);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    PatternRecognitionNode that = (PatternRecognitionNode) o;
    return Objects.equals(partitionBy, that.partitionBy)
        && Objects.equals(orderingScheme, that.orderingScheme)
        && Objects.equals(hashSymbol, that.hashSymbol)
        && Objects.equals(measures, that.measures)
        && rowsPerMatch == that.rowsPerMatch
        && Objects.equals(skipToLabels, that.skipToLabels)
        && skipToPosition == that.skipToPosition
        && Objects.equals(pattern, that.pattern)
        && Objects.equals(variableDefinitions, that.variableDefinitions);
  }

  public static PatternRecognitionNode deserialize(ByteBuffer byteBuffer) {

    int partitionSize = ReadWriteIOUtils.readInt(byteBuffer);
    ImmutableList.Builder<Symbol> partitionByBuilder = ImmutableList.builder();
    for (int i = 0; i < partitionSize; i++) {
      partitionByBuilder.add(Symbol.deserialize(byteBuffer));
    }
    List<Symbol> partitionBy = partitionByBuilder.build();

    Optional<OrderingScheme> orderingScheme;
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      orderingScheme = Optional.of(OrderingScheme.deserialize(byteBuffer));
    } else {
      orderingScheme = Optional.empty();
    }

    Optional<Symbol> hashSymbol;
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      hashSymbol = Optional.of(Symbol.deserialize(byteBuffer));
    } else {
      hashSymbol = Optional.empty();
    }

    int measureSize = ReadWriteIOUtils.readInt(byteBuffer);
    ImmutableMap.Builder<Symbol, Measure> measuresBuilder = ImmutableMap.builder();
    for (int i = 0; i < measureSize; i++) {
      Symbol key = Symbol.deserialize(byteBuffer);
      Measure value = Measure.deserialize(byteBuffer);
      measuresBuilder.put(key, value);
    }
    Map<Symbol, Measure> measures = measuresBuilder.build();

    RowsPerMatch rowsPerMatch = RowsPerMatch.deserialize(byteBuffer);

    int skipToLabelSize = ReadWriteIOUtils.readInt(byteBuffer);
    ImmutableSet.Builder<IrLabel> skipToLabelBuilder = ImmutableSet.builder();
    for (int i = 0; i < skipToLabelSize; i++) {
      skipToLabelBuilder.add(IrLabel.deserialize(byteBuffer));
    }
    Set<IrLabel> skipToLabels = skipToLabelBuilder.build();

    SkipToPosition skipToPosition = SkipToPosition.deserialize(byteBuffer);

    IrRowPattern pattern = IrRowPattern.deserialize(byteBuffer);

    int varDefSize = ReadWriteIOUtils.readInt(byteBuffer);
    ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> varDefBuilder =
        ImmutableMap.builder();
    for (int i = 0; i < varDefSize; i++) {
      IrLabel label = IrLabel.deserialize(byteBuffer);
      ExpressionAndValuePointers expr = ExpressionAndValuePointers.deserialize(byteBuffer);
      varDefBuilder.put(label, expr);
    }
    Map<IrLabel, ExpressionAndValuePointers> variableDefinitions = varDefBuilder.build();

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    return new PatternRecognitionNode(
        planNodeId,
        null,
        partitionBy,
        orderingScheme,
        hashSymbol,
        measures,
        rowsPerMatch,
        skipToLabels,
        skipToPosition,
        pattern,
        variableDefinitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        partitionBy,
        orderingScheme,
        hashSymbol,
        measures,
        rowsPerMatch,
        skipToLabels,
        skipToPosition,
        pattern,
        variableDefinitions);
  }

  @Override
  public String toString() {
    return "PatternRecognitionNode-" + this.getPlanNodeId();
  }
}
