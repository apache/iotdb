package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TopKRankingOperator implements ProcessOperator {
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TopKRankingNode.RankingType rankingType;
  private final List<TSDataType> inputTypes;

  private final List<Integer> outputChannels;
  private final List<Integer> partitionChannels;
  private final List<TSDataType> partitionTypes;
  private final List<Integer> sortChannels;
  private final List<SortItem> sortItems;
  private final int maxRowCountPerPartition;
  private final boolean partial;
  private final boolean generateRanking;
  private final Optional<Integer> hashChannel;
  private final int expectedPositions;

  private final long maxFlushableBytes;

  private final Supplier<GroupByHash> groupByHashSupplier;
  private final Supplier<GroupedTopNBuilder> groupedTopNBuilderSupplier;

  private GroupByHash groupByHash;
  private GroupedTopNBuilder groupedTopNBuilder;
  private boolean finishing;
  private java.util.Iterator<TsBlock> outputIterator;

  public TopKRankingOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      TopKRankingNode.RankingType rankingType,
      List<TSDataType> inputTypes,
      List<Integer> outputChannels,
      List<Integer> partitionChannels,
      List<TSDataType> partitionTypes,
      List<Integer> sortChannels,
      List<SortItem> sortItems,
      int maxRowCountPerPartition,
      boolean generateRanking,
      Optional<Integer> hashChannel,
      int expectedPositions,
      Optional<Long> maxPartialMemory) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.rankingType = rankingType;
    this.inputTypes = inputTypes;
    this.partitionChannels = partitionChannels;
    this.partitionTypes = partitionTypes;
    this.sortChannels = sortChannels;
    this.sortItems = sortItems;
    this.maxRowCountPerPartition = maxRowCountPerPartition;
    this.partial = !generateRanking;
    this.generateRanking = generateRanking;
    this.hashChannel = hashChannel;
    this.expectedPositions = expectedPositions;
    this.maxFlushableBytes = maxPartialMemory.orElse(0L);

    ImmutableList.Builder<Integer> outputChannelsBuilder = ImmutableList.builder();
    for (int channel : outputChannels) {
      outputChannelsBuilder.add(channel);
    }
    if (generateRanking) {
      outputChannelsBuilder.add(outputChannels.size());
    }
    this.outputChannels = outputChannelsBuilder.build();

    this.groupByHashSupplier =
        getGroupByHashSupplier(
            expectedPositions,
            partitionTypes,
            hashChannel.isPresent(),
            operatorContext.getSessionInfo(),
            UpdateMemory.NOOP);

    // Prepare grouped topN builder supplier
    this.groupedTopNBuilderSupplier =
        getGroupedTopNBuilderSupplier(
            rankingType,
            inputTypes,
            partitionChannels,
            sortChannels,
            sortItems,
            maxRowCountPerPartition,
            generateRanking,
            groupByHashSupplier);
  }

  private static Supplier<GroupByHash> getGroupByHashSupplier(
      int expectedPositions,
      List<TSDataType> partitionTsDataTypes,
      boolean hasPrecomputedHash,
      SessionInfo session,
      UpdateMemory updateMemory) {

    if (partitionTsDataTypes.isEmpty()) {
      return Suppliers.ofInstance(new NoChannelGroupByHash());
    }

    List<Type> partitionTypes = new ArrayList<>(partitionTsDataTypes.size());
    for (TSDataType partitionTsDataType : partitionTsDataTypes) {
      partitionTypes.add(InternalTypeManager.fromTSDataType(partitionTsDataType));
    }

    return () ->
        GroupByHash.createGroupByHash(
            partitionTypes, hasPrecomputedHash, expectedPositions, updateMemory);
  }

  private static Supplier<GroupedTopNBuilder> getGroupedTopNBuilderSupplier(
      TopKRankingNode.RankingType rankingType,
      List<TSDataType> sourceTypes,
      List<Integer> partitionChannels,
      List<Integer> sortChannels,
      List<SortItem> sortItems,
      int maxRankingPerPartition,
      boolean generateRanking,
      Supplier<GroupByHash> groupByHashSupplier) {

    if (rankingType == TopKRankingNode.RankingType.ROW_NUMBER) {
      TsBlockWithPositionComparator comparator =
          new SimpleTsBlockWithPositionComparator(sourceTypes, sortChannels, sortItems);
      return () ->
          new GroupedTopNRowNumberBuilder(
              sourceTypes,
              comparator,
              maxRankingPerPartition,
              generateRanking,
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
    if (!finishing && (!partial || !isBuilderFull()) && outputIterator == null) {
      // Still collecting input, nothing to output yet
      return null;
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

  @Override
  public boolean hasNext() throws Exception {
    // If we have an output iterator with more data, return true
    if (outputIterator != null && outputIterator.hasNext()) {
      return true;
    }

    // If we're finishing and have no more output, return false
    if (finishing && outputIterator == null && groupedTopNBuilder == null) {
      return false;
    }

    // If we have a builder that's full (partial) or we're finishing, we should have output
    return (partial && isBuilderFull()) || finishing;
  }

  @Override
  public void close() throws Exception {
    closeGroupedTopNBuilder();
    if (inputOperator != null) {
      inputOperator.close();
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finishing && outputIterator == null && groupedTopNBuilder == null;
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

  private void closeGroupedTopNBuilder() {
    if (groupedTopNBuilder != null) {
      groupedTopNBuilder = null;
    }
    if (groupByHash != null) {
      groupByHash = null;
    }
    outputIterator = null;
  }

  private boolean isBuilderFull() {
    return groupedTopNBuilder != null
        && groupedTopNBuilder.getEstimatedSizeInBytes() >= maxFlushableBytes;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
