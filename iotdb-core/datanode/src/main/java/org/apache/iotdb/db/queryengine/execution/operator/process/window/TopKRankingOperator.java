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

package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.GroupedTopNBuilder;
import org.apache.iotdb.db.queryengine.execution.operator.GroupedTopNRowNumberBuilder;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.SimpleTsBlockWithPositionComparator;
import org.apache.iotdb.db.queryengine.execution.operator.TsBlockWithPositionComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.NoChannelGroupByHash;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TopKRankingOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TopKRankingOperator.class);

  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TopKRankingNode.RankingType rankingType;
  private final List<TSDataType> inputTypes;

  private final List<Integer> outputChannels;
  private final List<Integer> partitionChannels;
  private final List<TSDataType> partitionTSDataTypes;
  private final List<Integer> sortChannels;
  private final List<SortOrder> sortOrders;
  private final int maxRowCountPerPartition;
  private final boolean partial;
  private final Optional<Integer> hashChannel;
  private final int expectedPositions;

  private final long maxFlushableBytes;

  private final Supplier<GroupByHash> groupByHashSupplier;
  private final Supplier<GroupedTopNBuilder> groupedTopNBuilderSupplier;

  private GroupByHash groupByHash;
  private GroupedTopNBuilder groupedTopNBuilder;

  private boolean finished = false;
  private java.util.Iterator<TsBlock> outputIterator;

  public TopKRankingOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      TopKRankingNode.RankingType rankingType,
      List<TSDataType> inputTypes,
      List<Integer> outputChannels,
      List<Integer> partitionChannels,
      List<TSDataType> partitionTSDataTypes,
      List<Integer> sortChannels,
      List<SortOrder> sortOrders,
      int maxRowCountPerPartition,
      boolean partial,
      Optional<Integer> hashChannel,
      int expectedPositions,
      Optional<Long> maxPartialMemory) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.rankingType = rankingType;
    this.inputTypes = inputTypes;
    this.partitionChannels = partitionChannels;
    this.partitionTSDataTypes = partitionTSDataTypes;
    this.sortChannels = sortChannels;
    this.sortOrders = sortOrders;
    this.maxRowCountPerPartition = maxRowCountPerPartition;
    this.partial = partial;
    this.hashChannel = hashChannel;
    this.expectedPositions = expectedPositions;
    this.maxFlushableBytes = maxPartialMemory.orElse(Long.MAX_VALUE);

    ImmutableList.Builder<Integer> outputChannelsBuilder = ImmutableList.builder();
    for (int channel : outputChannels) {
      outputChannelsBuilder.add(channel);
    }
    if (partial) {
      outputChannelsBuilder.add(outputChannels.size());
    }
    this.outputChannels = outputChannelsBuilder.build();

    this.groupByHashSupplier = getGroupByHashSupplier();
    this.groupedTopNBuilderSupplier = getGroupedTopNBuilderSupplier();
  }

  private Supplier<GroupByHash> getGroupByHashSupplier() {
    boolean hasPrecomputedHash = hashChannel.isPresent();

    if (partitionTSDataTypes.isEmpty()) {
      return Suppliers.ofInstance(new NoChannelGroupByHash());
    }

    List<Type> partitionTypes = new ArrayList<>(partitionTSDataTypes.size());
    for (TSDataType partitionTSDataType : partitionTSDataTypes) {
      partitionTypes.add(InternalTypeManager.fromTSDataType(partitionTSDataType));
    }

    return () ->
        GroupByHash.createGroupByHash(
            partitionTypes, hasPrecomputedHash, expectedPositions, UpdateMemory.NOOP);
  }

  private Supplier<GroupedTopNBuilder> getGroupedTopNBuilderSupplier() {
    if (rankingType == TopKRankingNode.RankingType.ROW_NUMBER) {
      TsBlockWithPositionComparator comparator =
          new SimpleTsBlockWithPositionComparator(inputTypes, sortChannels, sortOrders);
      return () ->
          new GroupedTopNRowNumberBuilder(
              inputTypes,
              comparator,
              maxRowCountPerPartition,
              !partial,
              partitionChannels.stream().mapToInt(Integer::intValue).toArray(),
              groupByHashSupplier.get());
    }

    //    if (rankingType == TopKRankingNode.RankingType.RANK) {
    //      Comparator<TsBlock> comparator = new SimpleTsBlockWithPositionComparator(
    //          sourceTypes, sortChannels, ascendingOrders);
    //      return () -> new GroupedTopNRankBuilder(
    //          sourceTypes,
    //          comparator,
    //          maxRankingPerPartition,
    //          generateRanking,
    //          groupByHashSupplier.get());
    //    }

    if (rankingType == TopKRankingNode.RankingType.DENSE_RANK) {
      throw new UnsupportedOperationException("DENSE_RANK not yet implemented");
    }

    throw new IllegalArgumentException("Unknown ranking type: " + rankingType);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    if (groupedTopNBuilder == null) {
      groupedTopNBuilder = groupedTopNBuilderSupplier.get();
    }

    if (!finished) {
      // If we are in partial mode and builder is full
      // we shall flush output immediately
      if (!partial || !isBuilderFull()) {
        // Feed all input TsBlocks to grouped TopK builder
        if (inputOperator.hasNextWithTimer()) {
          TsBlock tsBlock = inputOperator.nextWithTimer();
          if (tsBlock == null) {
            return null;
          }
          groupedTopNBuilder.addTsBlock(tsBlock);
          return null;
        } else {
          finished = true;
        }
      }
    }

    if (outputIterator == null && groupedTopNBuilder != null) {
      // Start flushing results
      outputIterator = groupedTopNBuilder.getResult();
    }

    if (outputIterator != null && outputIterator.hasNext()) {
      return outputIterator.next();
    } else {
      closeGroupedTopNBuilder();
      return null;
    }
  }

  private boolean isBuilderFull() {
    return groupedTopNBuilder != null
        && groupedTopNBuilder.getEstimatedSizeInBytes() >= maxFlushableBytes;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished || outputIterator != null;
  }

  @Override
  public void close() throws Exception {
    closeGroupedTopNBuilder();
    if (inputOperator != null) {
      inputOperator.close();
    }
  }

  private void closeGroupedTopNBuilder() {
    if (groupedTopNBuilder != null) {
      groupedTopNBuilder = null;
    }
    if (groupByHash != null) {
      groupByHash = null;
    }
    outputIterator = null;
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished && outputIterator == null && groupedTopNBuilder == null;
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemoryFromInput = inputOperator.calculateMaxPeekMemoryWithCounter();
    long maxTsBlockMemory = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    long builderMemory =
        groupedTopNBuilder != null ? groupedTopNBuilder.getEstimatedSizeInBytes() : 0;
    return Math.max(maxTsBlockMemory + builderMemory, maxPeekMemoryFromInput)
        + inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long retainedSize = inputOperator.calculateRetainedSizeAfterCallingNext();
    if (groupedTopNBuilder != null) {
      retainedSize += groupedTopNBuilder.getEstimatedSizeInBytes();
    }

    return retainedSize;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + (groupedTopNBuilder != null ? groupedTopNBuilder.getEstimatedSizeInBytes() : 0);
  }
}
