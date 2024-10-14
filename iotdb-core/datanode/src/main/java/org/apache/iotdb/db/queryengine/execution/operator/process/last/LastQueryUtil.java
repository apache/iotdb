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

package org.apache.iotdb.db.queryengine.execution.operator.process.last;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.queryengine.execution.aggregation.LastValueDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.aggregation.MaxTimeDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeGt;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeGtEq;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LastQueryUtil {

  private static final boolean CACHE_ENABLED =
      CommonDescriptor.getInstance().getConfig().isLastCacheEnable();

  private LastQueryUtil() {
    // util class doesn't need constructor
  }

  public static TsBlockBuilder createTsBlockBuilder() {
    return new TsBlockBuilder(ImmutableList.of(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));
  }

  public static TsBlockBuilder createTsBlockBuilder(int initialExpectedEntries) {
    return new TsBlockBuilder(
        initialExpectedEntries,
        ImmutableList.of(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));
  }

  public static Binary getTimeSeries(TsBlock tsBlock, int index) {
    return tsBlock.getColumn(0).getBinary(index);
  }

  public static void appendLastValue(
      TsBlockBuilder builder, long lastTime, String fullPath, String lastValue, String dataType) {
    // Time
    builder.getTimeColumnBuilder().writeLong(lastTime);
    // timeseries
    builder.getColumnBuilder(0).writeBinary(new Binary(fullPath, TSFileConfig.STRING_CHARSET));
    // value
    builder.getColumnBuilder(1).writeBinary(new Binary(lastValue, TSFileConfig.STRING_CHARSET));
    // dataType
    builder.getColumnBuilder(2).writeBinary(new Binary(dataType, TSFileConfig.STRING_CHARSET));
    builder.declarePosition();
  }

  public static void appendLastValue(
      TsBlockBuilder builder, long lastTime, Binary fullPath, String lastValue, String dataType) {
    // Time
    builder.getTimeColumnBuilder().writeLong(lastTime);
    // timeseries
    builder.getColumnBuilder(0).writeBinary(fullPath);
    // value
    builder.getColumnBuilder(1).writeBinary(new Binary(lastValue, TSFileConfig.STRING_CHARSET));
    // dataType
    builder.getColumnBuilder(2).writeBinary(new Binary(dataType, TSFileConfig.STRING_CHARSET));
    builder.declarePosition();
  }

  public static void appendLastValue(TsBlockBuilder builder, TsBlock tsBlock) {
    if (tsBlock.isEmpty()) {
      return;
    }
    int size = tsBlock.getPositionCount();
    for (int i = 0; i < size; i++) {
      // Time
      builder.getTimeColumnBuilder().writeLong(tsBlock.getTimeByIndex(i));
      // timeseries
      builder.getColumnBuilder(0).writeBinary(tsBlock.getColumn(0).getBinary(i));
      // value
      builder.getColumnBuilder(1).writeBinary(tsBlock.getColumn(1).getBinary(i));
      // dataType
      builder.getColumnBuilder(2).writeBinary(tsBlock.getColumn(2).getBinary(i));
      builder.declarePosition();
    }
  }

  public static void appendLastValue(TsBlockBuilder builder, TsBlock tsBlock, int index) {
    // Time
    builder.getTimeColumnBuilder().writeLong(tsBlock.getTimeByIndex(index));
    // timeseries
    builder.getColumnBuilder(0).writeBinary(tsBlock.getColumn(0).getBinary(index));
    // value
    builder.getColumnBuilder(1).writeBinary(tsBlock.getColumn(1).getBinary(index));
    // dataType
    builder.getColumnBuilder(2).writeBinary(tsBlock.getColumn(2).getBinary(index));
    builder.declarePosition();
  }

  public static int compareTimeSeries(
      TsBlock a, int indexA, TsBlock b, int indexB, Comparator<Binary> comparator) {
    return comparator.compare(a.getColumn(0).getBinary(indexA), b.getColumn(0).getBinary(indexB));
  }

  public static boolean satisfyFilter(Filter filter, TimeValuePair tvPair) {
    return filter == null || filter.satisfy(tvPair.getTimestamp(), tvPair.getValue().getValue());
  }

  public static List<TreeAggregator> createAggregators(TSDataType dataType) {
    // max_time, last_value
    List<TreeAggregator> aggregators = new ArrayList<>(2);
    aggregators.add(
        new TreeAggregator(
            new MaxTimeDescAccumulator(),
            AggregationStep.SINGLE,
            Collections.singletonList(new InputLocation[] {new InputLocation(0, 0)})));
    aggregators.add(
        new TreeAggregator(
            new LastValueDescAccumulator(dataType),
            AggregationStep.SINGLE,
            Collections.singletonList(new InputLocation[] {new InputLocation(0, 0)})));
    return aggregators;
  }

  public static List<TreeAggregator> createAggregators(TSDataType dataType, int valueColumnIndex) {
    // max_time, last_value
    List<TreeAggregator> aggregators = new ArrayList<>(2);
    aggregators.add(
        new TreeAggregator(
            new MaxTimeDescAccumulator(),
            AggregationStep.SINGLE,
            Collections.singletonList(
                new InputLocation[] {new InputLocation(0, valueColumnIndex)})));
    aggregators.add(
        new TreeAggregator(
            new LastValueDescAccumulator(dataType),
            AggregationStep.SINGLE,
            Collections.singletonList(
                new InputLocation[] {new InputLocation(0, valueColumnIndex)})));
    return aggregators;
  }

  public static boolean needUpdateCache(final Filter timeFilter) {
    // Update the cache only when, the filter is gt (greater than) or ge (greater than or equal to)
    return CACHE_ENABLED
        && (timeFilter == null || timeFilter instanceof TimeGtEq || timeFilter instanceof TimeGt);
  }

  public static boolean needUpdateNullEntry(final Filter timeFilter) {
    return CACHE_ENABLED && timeFilter == null;
  }
}
