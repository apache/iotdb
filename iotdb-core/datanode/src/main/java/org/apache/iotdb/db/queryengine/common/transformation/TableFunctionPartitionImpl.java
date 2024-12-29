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

package org.apache.iotdb.db.queryengine.common.transformation;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.TableFunctionPartition;
import org.apache.iotdb.udf.api.type.Type;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;

import static java.lang.String.format;

public class TableFunctionPartitionImpl implements TableFunctionPartition {

  private final List<Column[]> columns;
  private final List<Integer> positionOffsets;
  private final int startPosition;
  private final int endPosition;
  private final List<Type> outputDataTypes;
  private final int properChannelCount;
  private final TableFunctionNode.PassThroughSpecification passThroughSpecifications;

  private int positionIndex;
  private int blockIndex;

  public TableFunctionPartitionImpl(
      List<TsBlock> tsBlocks,
      int startPosition,
      int endPosition,
      List<Integer> requiredChannels,
      List<TSDataType> outputDataTypes,
      int properChannelCount,
      TableFunctionNode.PassThroughSpecification passThroughSpecifications) {
    if (requiredChannels.size() != outputDataTypes.size()) {
      throw new IllegalArgumentException("requiredChannels.size() != outputDataTypes.size()");
    }
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    ImmutableList.Builder<Column[]> columnsBuilder = ImmutableList.builder();
    ImmutableList.Builder<Integer> positionBuilder = ImmutableList.builder();
    int prefixSum = 0;
    for (TsBlock tsBlock : tsBlocks) {
      Column[] allColumns = tsBlock.getAllColumns();
      // TODO(UDF): 需要确认一下这边的顺序，time列放在最后和预期是否一致
      columnsBuilder.add(requiredChannels.stream().map(i -> allColumns[i]).toArray(Column[]::new));
      positionBuilder.add(prefixSum);
      prefixSum += tsBlock.getPositionCount();
    }
    if (endPosition > prefixSum) {
      throw new IllegalArgumentException("endPosition > prefixSum");
    }
    this.columns = columnsBuilder.build();
    this.positionOffsets = positionBuilder.build();
    this.positionIndex = startPosition;
    this.blockIndex = 0;
    this.outputDataTypes = UDFDataTypeTransformer.transformToUDFDataTypeList(outputDataTypes);
    this.properChannelCount = properChannelCount;
    this.passThroughSpecifications = passThroughSpecifications;
  }

  /** Connect proper columns and pass through columns to construct the result. */
  public TsBlock constructResult(List<Column> inputs) {
    if (inputs.size() != properChannelCount + (passThroughSpecifications == null ? 0 : 1)) {
      throw new RuntimeException(
          format(
              "inputs.size() != properChannelCount + passThroughSourceCount, %d != %d + %d",
              inputs.size(), properChannelCount, (passThroughSpecifications == null ? 0 : 1)));
    }
    // TODO(UDF): implement this method
    return null;
  }

  @Override
  public int size() {
    return endPosition - startPosition + 1;
  }
}
