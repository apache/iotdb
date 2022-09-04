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

package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.ValueIterator;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class GroupByWithValueFilterDataSet extends GroupByEngineDataSet {

  private Map<IReaderByTimestamp, List<List<Integer>>> readerToAggrIndexesMap;

  protected GroupByTimePlan groupByTimePlan;
  private TimeGenerator timestampGenerator;
  /** cached timestamp for next group by partition. */
  private LinkedList<Long> cachedTimestamps = new LinkedList<>();
  /** group by batch calculation size. */
  protected int timeStampFetchSize;

  private long lastTimestamp;

  protected GroupByWithValueFilterDataSet() {}

  /** constructor. */
  public GroupByWithValueFilterDataSet(QueryContext context, GroupByTimePlan groupByTimePlan) {
    super(context, groupByTimePlan);
    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
  }

  @TestOnly
  public GroupByWithValueFilterDataSet(long queryId, GroupByTimePlan groupByTimePlan) {
    super(new QueryContext(queryId), groupByTimePlan);
    this.readerToAggrIndexesMap = new HashMap<>();
    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
  }

  /** init reader and aggregate function. This method should be called once after initializing */
  public void initGroupBy(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {
    this.timestampGenerator = getTimeGenerator(context, groupByTimePlan);
    this.readerToAggrIndexesMap = new HashMap<>();
    this.groupByTimePlan = groupByTimePlan;

    List<PartialPath> selectedSeries = new ArrayList<>();
    groupByTimePlan
        .getDeduplicatedPaths()
        .forEach(k -> selectedSeries.add(((MeasurementPath) k).transformToExactPath()));

    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        MetaUtils.groupAggregationsBySeries(selectedSeries);
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap =
        MetaUtils.groupAlignedSeriesWithAggregations(pathToAggrIndexesMap);

    List<VirtualStorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLock(paths.stream().map(p -> (PartialPath) p).collect(Collectors.toList()));
    try {
      // init non-aligned series reader
      for (PartialPath path : pathToAggrIndexesMap.keySet()) {
        IReaderByTimestamp seriesReaderByTimestamp =
            getReaderByTime(path, groupByTimePlan, context);
        readerToAggrIndexesMap.put(
            seriesReaderByTimestamp, Collections.singletonList(pathToAggrIndexesMap.get(path)));
      }
      // init aligned series reader
      for (PartialPath alignedPath : alignedPathToAggrIndexesMap.keySet()) {
        IReaderByTimestamp seriesReaderByTimestamp =
            getReaderByTime(alignedPath, groupByTimePlan, context);
        readerToAggrIndexesMap.put(
            seriesReaderByTimestamp, alignedPathToAggrIndexesMap.get(alignedPath));
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);

      // assign null to be friendly for GC
      pathToAggrIndexesMap = null;
      alignedPathToAggrIndexesMap = null;
    }
  }

  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    return new ServerTimeGenerator(context, queryPlan);
  }

  protected IReaderByTimestamp getReaderByTime(
      PartialPath path, RawDataQueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return new SeriesReaderByTimestamp(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        path.getSeriesType(),
        context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null),
        null,
        ascending);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next()" + " in GroupByWithValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    curAggregateResults = new AggregateResult[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      curAggregateResults[i] =
          AggregateResultFactory.getAggrResultByName(
              groupByTimePlan.getDeduplicatedAggregations().get(i),
              groupByTimePlan.getDeduplicatedDataTypes().get(i),
              ascending);
    }

    long[] timestampArray = new long[timeStampFetchSize];
    int timeArrayLength = 0;

    if (!cachedTimestamps.isEmpty()) {
      long timestamp = cachedTimestamps.remove();
      if (timestamp < curEndTime) {
        if (!groupByTimePlan.isAscending() && timestamp < curStartTime) {
          cachedTimestamps.addFirst(timestamp);
          return constructRowRecord(curAggregateResults);
        }
        if (timestamp >= curStartTime) {
          timestampArray[timeArrayLength++] = timestamp;
        }
      } else {
        cachedTimestamps.addFirst(timestamp);
        return constructRowRecord(curAggregateResults);
      }
    }

    while (!cachedTimestamps.isEmpty() || timestampGenerator.hasNext()) {
      // construct timestamp array
      timeArrayLength = constructTimeArrayForOneCal(timestampArray, timeArrayLength);

      // cal result using timestamp array
      calcUsingTimestampArray(timestampArray, timeArrayLength);

      timeArrayLength = 0;
      // judge if it's end
      if ((groupByTimePlan.isAscending() && lastTimestamp >= curEndTime)
          || (!groupByTimePlan.isAscending() && lastTimestamp < curStartTime)) {
        break;
      }
    }

    if (timeArrayLength > 0) {
      // cal result using timestamp array
      calcUsingTimestampArray(timestampArray, timeArrayLength);
    }
    return constructRowRecord(curAggregateResults);
  }

  private void calcUsingTimestampArray(long[] timestampArray, int timeArrayLength)
      throws IOException {
    for (Entry<IReaderByTimestamp, List<List<Integer>>> entry : readerToAggrIndexesMap.entrySet()) {
      IReaderByTimestamp reader = entry.getKey();
      List<List<Integer>> subIndexes = entry.getValue();
      int subSensorSize = subIndexes.size();

      Object[] values = reader.getValuesInTimestamps(timestampArray, timeArrayLength);
      ValueIterator valueIterator = QueryUtils.generateValueIterator(values);
      if (valueIterator != null) {
        for (int curIndex = 0; curIndex < subSensorSize; curIndex++) {
          valueIterator.setSubMeasurementIndex(curIndex);
          for (Integer index : subIndexes.get(curIndex)) {
            curAggregateResults[index].updateResultUsingValues(
                timestampArray, timeArrayLength, valueIterator);
            valueIterator.reset();
          }
        }
      }
    }
  }

  /**
   * construct an array of timestamps for one batch of a group by partition calculating.
   *
   * @param timestampArray timestamp array
   * @param timeArrayLength the current size of timestamp array
   * @return time array size
   */
  @SuppressWarnings("squid:S3776")
  private int constructTimeArrayForOneCal(long[] timestampArray, int timeArrayLength)
      throws IOException {
    for (int cnt = 1;
        cnt < timeStampFetchSize - 1
            && (!cachedTimestamps.isEmpty() || timestampGenerator.hasNext());
        cnt++) {
      if (!cachedTimestamps.isEmpty()) {
        lastTimestamp = cachedTimestamps.remove();
      } else {
        lastTimestamp = timestampGenerator.next();
      }
      if (groupByTimePlan.isAscending() && lastTimestamp < curEndTime) {
        timestampArray[timeArrayLength++] = lastTimestamp;
      } else if (!groupByTimePlan.isAscending() && lastTimestamp >= curStartTime) {
        timestampArray[timeArrayLength++] = lastTimestamp;
      } else {
        // may lastTimestamp get from cache
        if (!cachedTimestamps.isEmpty() && lastTimestamp <= cachedTimestamps.peek()) {
          cachedTimestamps.addFirst(lastTimestamp);
        } else {
          cachedTimestamps.add(lastTimestamp);
        }
        break;
      }
    }
    return timeArrayLength;
  }

  private RowRecord constructRowRecord(AggregateResult[] aggregateResultList) {
    RowRecord record;
    if (leftCRightO) {
      record = new RowRecord(curStartTime);
    } else {
      record = new RowRecord(curEndTime - 1);
    }
    for (int i = 0; i < paths.size(); i++) {
      AggregateResult aggregateResult = aggregateResultList[i];
      record.addField(aggregateResult.getResult(), aggregateResult.getResultDataType());
    }
    return record;
  }
}
