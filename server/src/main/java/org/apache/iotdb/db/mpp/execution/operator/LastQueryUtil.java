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
package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.LastValueDescAccumulator;
import org.apache.iotdb.db.mpp.aggregation.MaxTimeDescAccumulator;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LastQueryUtil {

  private static final boolean CACHE_ENABLED =
      IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled();

  public static TsBlockBuilder createTsBlockBuilder() {
    return new TsBlockBuilder(ImmutableList.of(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));
  }

  public static TsBlockBuilder createTsBlockBuilder(int initialExpectedEntries) {
    return new TsBlockBuilder(
        initialExpectedEntries,
        ImmutableList.of(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));
  }

  public static void appendLastValue(
      TsBlockBuilder builder, long lastTime, String fullPath, String lastValue, String dataType) {
    // Time
    builder.getTimeColumnBuilder().writeLong(lastTime);
    // timeseries
    builder.getColumnBuilder(0).writeBinary(new Binary(fullPath));
    // value
    builder.getColumnBuilder(1).writeBinary(new Binary(lastValue));
    // dataType
    builder.getColumnBuilder(2).writeBinary(new Binary(dataType));
    builder.declarePosition();
  }

  public static boolean satisfyFilter(Filter filter, TimeValuePair tvPair) {
    return filter == null || filter.satisfy(tvPair.getTimestamp(), tvPair.getValue().getValue());
  }

  public static List<Aggregator> createAggregators(TSDataType dataType) {
    // max_time, last_value
    List<Aggregator> aggregators = new ArrayList<>(2);
    aggregators.add(
        new Aggregator(
            new MaxTimeDescAccumulator(),
            AggregationStep.SINGLE,
            Collections.singletonList(new InputLocation[] {new InputLocation(0, 0)})));
    aggregators.add(
        new Aggregator(
            new LastValueDescAccumulator(dataType),
            AggregationStep.SINGLE,
            Collections.singletonList(new InputLocation[] {new InputLocation(0, 0)})));
    return aggregators;
  }

  public static boolean needUpdateCache(Filter timeFilter) {
    // Update the cache only when, the filter is gt (greater than) or ge (greater than or equal to)
    return CACHE_ENABLED && (timeFilter == null || timeFilter instanceof GtEq)
        || (timeFilter instanceof Gt);
  }
}
