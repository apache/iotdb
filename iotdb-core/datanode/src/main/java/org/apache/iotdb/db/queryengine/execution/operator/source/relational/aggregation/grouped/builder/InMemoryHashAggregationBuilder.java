/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.builder;

import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash.createGroupByHash;

public class InMemoryHashAggregationBuilder implements HashAggregationBuilder {
  private final int[] groupByChannels;
  private GroupByHash groupByHash;
  private final List<Type> groupByOutputTypes;
  private final List<GroupedAggregator> groupedAggregators;
  private final boolean partial;
  private final OptionalLong maxPartialMemory;
  private final UpdateMemory updateMemory;

  private boolean full;

  private Iterator<Integer> groupIds;
  private final TsBlockBuilder pageBuilder;

  private final int expectedGroups;
  private final Optional<Integer> hashChannel;

  private final OperatorContext operatorContext;

  private static final String CURRENT_GROUP_NUMBER = "CurrentGroupNumber";
  private static final String MAX_GROUP_NUMBER = "MaxGroupNumber";
  private long maxGroupNumber;

  public InMemoryHashAggregationBuilder(
      List<GroupedAggregator> groupedAggregators,
      AggregationNode.Step step,
      int expectedGroups,
      List<Type> groupByTypes,
      List<Integer> groupByChannels,
      Optional<Integer> hashChannel,
      OperatorContext operatorContext,
      long maxPartialMemory,
      UpdateMemory updateMemory) {
    this(
        groupedAggregators,
        step,
        expectedGroups,
        groupByTypes,
        groupByChannels,
        hashChannel,
        operatorContext,
        maxPartialMemory,
        Optional.empty(),
        updateMemory);
  }

  public InMemoryHashAggregationBuilder(
      List<GroupedAggregator> groupedAggregators,
      AggregationNode.Step step,
      int expectedGroups,
      List<Type> groupByTypes,
      List<Integer> groupByChannels,
      Optional<Integer> hashChannel,
      OperatorContext operatorContext,
      long maxPartialMemory,
      Optional<Integer> unspillIntermediateChannelOffset,
      UpdateMemory updateMemory) {
    this.groupedAggregators = groupedAggregators;
    this.groupByOutputTypes = ImmutableList.copyOf(groupByTypes);
    this.groupByChannels = Ints.toArray(groupByChannels);

    this.groupByHash =
        createGroupByHash(groupByTypes, hashChannel.isPresent(), expectedGroups, updateMemory);
    this.partial = step.isOutputPartial();
    this.maxPartialMemory = OptionalLong.of(maxPartialMemory);
    this.updateMemory = updateMemory;

    this.pageBuilder = new TsBlockBuilder(buildTypes());

    this.expectedGroups = expectedGroups;
    this.hashChannel = hashChannel;

    this.operatorContext = operatorContext;
  }

  @Override
  public void close() {}

  @Override
  public void processBlock(TsBlock block) {
    if (groupedAggregators.isEmpty()) {
      groupByHash.addPage(block.getColumns(groupByChannels));
    }
    int[] groupByIdBlock = groupByHash.getGroupIds(block.getColumns(groupByChannels));

    int groupCount = groupByHash.getGroupCount();
    operatorContext.recordSpecifiedInfo(CURRENT_GROUP_NUMBER, Long.toString(groupCount));
    if (groupCount > maxGroupNumber) {
      operatorContext.recordSpecifiedInfo(MAX_GROUP_NUMBER, Long.toString(groupCount));
      maxGroupNumber = groupCount;
    }
    for (GroupedAggregator groupedAggregator : groupedAggregators) {
      groupedAggregator.processBlock(groupCount, groupByIdBlock, block);
    }
  }

  @Override
  public void updateMemory() {
    //  updateMemory.update();
  }

  @Override
  public void reset() {
    // TODO reset of GroupByHash
    groupByHash =
        createGroupByHash(
            groupByOutputTypes, hashChannel.isPresent(), expectedGroups, updateMemory);
    groupedAggregators.forEach(GroupedAggregator::reset);
    full = false;
    groupIds = null;
    pageBuilder.reset();
  }

  @Override
  public boolean isFull() {
    return full;
  }

  public long getEstimatedSize() {
    long sizeInMemory = groupByHash.getEstimatedSize();
    for (GroupedAggregator groupedAggregator : groupedAggregators) {
      sizeInMemory += groupedAggregator.getEstimatedSize();
    }

    updateIsFull(sizeInMemory);
    return sizeInMemory;
  }

  private void updateIsFull(long sizeInMemory) {
    if (!partial || !maxPartialMemory.isPresent()) {
      return;
    }

    full = sizeInMemory > maxPartialMemory.getAsLong();
  }

  /**
   * building hash sorted results requires memory for sorting group IDs. This method returns size of
   * that memory requirement.
   */
  public long getGroupIdsSortingSize() {
    return getGroupCount() * Integer.BYTES;
  }

  public void setSpillOutput() {
    for (GroupedAggregator groupedAggregator : groupedAggregators) {
      // groupedAggregator.setSpillOutput();
    }
  }

  public int getKeyChannels() {
    return groupByChannels.length;
  }

  public long getGroupCount() {
    return groupByHash.getGroupCount();
  }

  @Override
  public TsBlock buildResult() {
    // used for order by in Agg
    /*for (GroupedAggregator groupedAggregator : groupedAggregators) {
      groupedAggregator.prepareFinal();
    }*/
    return buildResult(consecutiveGroupIds());
  }

  @Override
  public boolean finished() {
    return !groupIds.hasNext();
  }

  public List<Type> buildSpillTypes() {
    ArrayList<Type> types = new ArrayList<>(groupByOutputTypes);
    for (GroupedAggregator groupedAggregator : groupedAggregators) {
      // types.add(groupedAggregator.getSpillType());
    }
    return types;
  }

  public int getCapacity() {
    return groupByHash.getCapacity();
  }

  private TsBlock buildResult(Iterator<Integer> groupIds) {
    pageBuilder.reset();

    while (!pageBuilder.isFull() && groupIds.hasNext()) {
      int groupId = groupIds.next();

      groupByHash.appendValuesTo(groupId, pageBuilder);

      pageBuilder.declarePosition();
      for (int i = 0; i < groupedAggregators.size(); i++) {
        GroupedAggregator groupedAggregator = groupedAggregators.get(i);
        ColumnBuilder output = pageBuilder.getColumnBuilder(groupByChannels.length + i);
        groupedAggregator.evaluate(groupId, output);
      }
    }

    return TsBlock.wrapBlocksWithoutCopy(
        pageBuilder.getPositionCount(),
        new RunLengthEncodedColumn(
            TableScanOperator.TIME_COLUMN_TEMPLATE, pageBuilder.getPositionCount()),
        Arrays.stream(pageBuilder.getValueColumnBuilders())
            .map(ColumnBuilder::build)
            .toArray(Column[]::new));
  }

  public List<TSDataType> buildTypes() {

    return Stream.concat(
            groupByOutputTypes.stream().map(InternalTypeManager::getTSDataType),
            groupedAggregators.stream().map(GroupedAggregator::getType))
        .collect(Collectors.toList());
  }

  private Iterator<Integer> consecutiveGroupIds() {
    if (groupIds == null) {
      groupIds = IntStream.range(0, groupByHash.getGroupCount()).iterator();
    }
    return groupIds;
  }
}
