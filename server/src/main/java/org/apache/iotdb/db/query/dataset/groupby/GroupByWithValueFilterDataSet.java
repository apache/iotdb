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
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class GroupByWithValueFilterDataSet extends GroupByEngineDataSet {

  private static final Logger logger = LoggerFactory.getLogger(GroupByWithValueFilterDataSet.class);

  private Map<List<IReaderByTimestamp>, List<List<Integer>>> readersToAggrIndexes;

  protected GroupByTimePlan groupByTimePlan;
  /** timestampGenerators for each iteration when aggregation needs */
  private List<TimeGenerator> timestampGenerators;
  /** cached timestamp for next group by partition. */
  private LinkedList<Long> cachedTimestamps = new LinkedList<>();
  /** group by batch calculation size. */
  protected int timeStampFetchSize;

  private long lastTimestamp;
  private int maxIteration;

  protected GroupByWithValueFilterDataSet() {}

  /** constructor. */
  public GroupByWithValueFilterDataSet(QueryContext context, GroupByTimePlan groupByTimePlan) {
    super(context, groupByTimePlan);
    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
  }

  @TestOnly
  public GroupByWithValueFilterDataSet(long queryId, GroupByTimePlan groupByTimePlan) {
    super(new QueryContext(queryId), groupByTimePlan);
    this.readersToAggrIndexes = new HashMap<>();
    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
  }

  /** init reader and aggregate function. This method should be called once after initializing */
  public void initGroupBy(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {
    this.readersToAggrIndexes = new HashMap<>();
    this.groupByTimePlan = groupByTimePlan;

    Filter timeFilter =
        FilterFactory.and(
            TimeFilter.gtEq(groupByTimePlan.getStartTime()),
            TimeFilter.lt(groupByTimePlan.getEndTime()));

    List<PartialPath> selectedSeries = new ArrayList<>();
    groupByTimePlan
        .getDeduplicatedPaths()
        .forEach(k -> selectedSeries.add(((MeasurementPath) k).transformToExactPath()));

    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        MetaUtils.groupAggregationsBySeries(selectedSeries);
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap =
        MetaUtils.groupAlignedSeriesWithAggregations(pathToAggrIndexesMap);

    List<PartialPath> groupedPathList =
        new ArrayList<>(pathToAggrIndexesMap.size() + alignedPathToAggrIndexesMap.size());
    groupedPathList.addAll(pathToAggrIndexesMap.keySet());
    groupedPathList.addAll(alignedPathToAggrIndexesMap.keySet());

    Pair<List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance().mergeLock(groupedPathList);
    List<VirtualStorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<VirtualStorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    maxIteration = 0;
    for (int i = 0; i < selectedSeries.size(); i++)
      maxIteration = Math.max(maxIteration, getMaxIterationFromAggrIndex(i));
    this.timestampGenerators = new ArrayList<>();
    for (int iteration = 0; iteration < maxIteration; iteration++) {
      this.timestampGenerators.add(getTimeGenerator(context, groupByTimePlan));
    }

    // init non-aligned series reader
    for (PartialPath path : pathToAggrIndexesMap.keySet()) {
      List<Integer> aggrIndexes = pathToAggrIndexesMap.get(path);
      int maxIterationInPath = getMaxIterationFromAggrIndexes(aggrIndexes);
      List<IReaderByTimestamp> seriesReadersByTimestamp =
          getReaderListByTime(maxIterationInPath, path, groupByTimePlan, context);
      readersToAggrIndexes.put(seriesReadersByTimestamp, Collections.singletonList(aggrIndexes));
    }
    // init aligned series reader
    for (AlignedPath alignedPath : alignedPathToAggrIndexesMap.keySet()) {
      List<List<Integer>> aggrIndexes = alignedPathToAggrIndexesMap.get(alignedPath);
      int maxIterationInDevice = getMaxIterationInDevice(aggrIndexes);
      List<IReaderByTimestamp> seriesReadersByTimestamp =
          getReaderListByTime(maxIterationInDevice, alignedPath, groupByTimePlan, context);
      readersToAggrIndexes.put(seriesReadersByTimestamp, aggrIndexes);
    }
    // assign null to be friendly for GC
    pathToAggrIndexesMap = null;
    alignedPathToAggrIndexesMap = null;
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
        QueryResourceManager.getInstance()
            .getQueryDataSource(path, context, null, queryPlan.isAscending()),
        null,
        ascending);
  }

  private int getMaxIterationFromAggrIndexes(List<Integer> aggrIndexes) {
    int maxIteration = 0;
    for (int aggrIndex : aggrIndexes)
      maxIteration = Math.max(maxIteration, getMaxIterationFromAggrIndex(aggrIndex));
    return maxIteration;
  }

  private int getMaxIterationInDevice(List<List<Integer>> aggrIndexes) {
    int maxIteration = 0;
    for (List<Integer> aggrIndexList : aggrIndexes)
      maxIteration = Math.max(maxIteration, getMaxIterationFromAggrIndexes(aggrIndexList));
    return maxIteration;
  }

  private int getMaxIterationFromAggrIndex(int aggrIndex) {
    return AggregateResultFactory.getAggrResultByName(
            groupByTimePlan.getDeduplicatedAggregations().get(aggrIndex),
            groupByTimePlan.getDeduplicatedDataTypes().get(aggrIndex),
            ascending)
        .maxIteration();
  }

  private List<IReaderByTimestamp> getReaderListByTime(
      int maxIteration, PartialPath path, RawDataQueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    List<IReaderByTimestamp> readerList = new ArrayList<>();
    for (int i = 0; i < maxIteration; i++)
      readerList.add(getReaderByTime(path, queryPlan, context));
    return readerList;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    //    System.out.println("[DEBUG][nextWithoutConstraint]" + "
    // cachedTimestamps:"+cachedTimestamps.toString()+"  ");
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

    if (!cachedTimestamps.isEmpty()) {
      long nextTimestamp = cachedTimestamps.peek();
      // nextTimestamp not in current interval
      if ((groupByTimePlan.isAscending() && nextTimestamp >= curEndTime)
          || (!groupByTimePlan.isAscending() && nextTimestamp < curStartTime))
        return constructRowRecord(curAggregateResults);
    }

    LinkedList<Long> currentCachedTimestamps = new LinkedList<>(cachedTimestamps);
    for (int iteration = 0; iteration < maxIteration; iteration++) {

      Set<Integer> remainingAggrIndexes = new HashSet<>();
      for (int i = 0; i < paths.size(); i++)
        if (!curAggregateResults[i].hasFinalResult()
            && curAggregateResults[i].maxIteration() > iteration) remainingAggrIndexes.add(i);
      if (remainingAggrIndexes.isEmpty()) break;

      for (Integer aggrIndex : remainingAggrIndexes)
        curAggregateResults[aggrIndex].startIteration();
      if (iteration > 0) currentCachedTimestamps = new LinkedList<>(cachedTimestamps);
      iterateCurrentInterval(
          iteration, currentCachedTimestamps, timestampArray, remainingAggrIndexes);
      for (Integer aggrIndex : remainingAggrIndexes)
        curAggregateResults[aggrIndex].finishIteration();
    }
    //    System.out.println("\t[DEBUG] \tcurrentCachedTimestamps:"+currentCachedTimestamps);
    cachedTimestamps = new LinkedList<>(currentCachedTimestamps);

    return constructRowRecord(curAggregateResults);
  }

  protected void iterateCurrentInterval(
      int iteration,
      LinkedList<Long> cachedTimestamps,
      long[] timestampArray,
      Set<Integer> remainingAggrIndexes)
      throws IOException {
    int timeArrayLength = 0;
    while (!cachedTimestamps.isEmpty() || timestampGenerators.get(iteration).hasNext()) {
      // construct timestamp array
      timeArrayLength = constructTimeArrayForOneCal(iteration, cachedTimestamps, timestampArray);
      //      System.out.println("\t\t[DEBUG] "+"timestampArray:"+
      //          Arrays.toString(Arrays.copyOf(timestampArray,timeArrayLength)));
      //      System.out.println("\t\t[DEBUG] "+"cachedTimestamps:"+cachedTimestamps);

      calcUsingTimestampArray(iteration, timestampArray, timeArrayLength, remainingAggrIndexes);

      if ((groupByTimePlan.isAscending() && lastTimestamp >= curEndTime)
          || (!groupByTimePlan.isAscending() && lastTimestamp < curStartTime)) {
        break;
      }
    }
  }

  private void calcUsingTimestampArray(
      int iteration, long[] timestampArray, int timeArrayLength, Set<Integer> remainingAggrIndexes)
      throws IOException {
    for (Entry<List<IReaderByTimestamp>, List<List<Integer>>> entry :
        readersToAggrIndexes.entrySet()) {
      List<IReaderByTimestamp> readers = entry.getKey();
      // maxIteration in device too small
      if (readers.size() <= iteration) continue;
      Set<Integer> remainingAggrIndexesInDevice =
          findRemainingAggrIndexesInDevice(remainingAggrIndexes, entry.getValue());
      // all aggregations in device have final result
      if (remainingAggrIndexesInDevice.isEmpty()) continue;
      IReaderByTimestamp reader = readers.get(iteration);
      List<List<Integer>> subIndexes = entry.getValue();
      int subSensorSize = subIndexes.size();

      Object[] values = reader.getValuesInTimestamps(timestampArray, timeArrayLength);
      ValueIterator valueIterator = QueryUtils.generateValueIterator(values);
      if (valueIterator != null) {
        for (int curIndex = 0; curIndex < subSensorSize; curIndex++) {
          Set<Integer> remainingIndexes =
              findRemainingAggrIndexesInSeries(
                  remainingAggrIndexesInDevice, subIndexes.get(curIndex));
          if (remainingIndexes.isEmpty()) continue;
          valueIterator.setSubMeasurementIndex(curIndex);
          for (Integer index : remainingIndexes) {
            curAggregateResults[index].updateResultUsingValues(
                timestampArray, timeArrayLength, valueIterator);
            valueIterator.reset();
          }
        }
      }
    }
  }
  // TODO: change. calc .
  private Set<Integer> findRemainingAggrIndexesInDevice(
      Set<Integer> remainingAggrIndexes, List<List<Integer>> subIndexes) {
    Set<Integer> remainingAggrIndexesInDevice = new HashSet<>();
    for (List<Integer> aggrIndexesInSeries : subIndexes)
      for (Integer aggrIndex : aggrIndexesInSeries)
        if (remainingAggrIndexes.contains(aggrIndex)) remainingAggrIndexesInDevice.add(aggrIndex);
    return remainingAggrIndexesInDevice;
  }

  private Set<Integer> findRemainingAggrIndexesInSeries(
      Set<Integer> remainingAggrIndexesInDevice, List<Integer> aggrIndexes) {
    Set<Integer> remainingAggrIndexesInSeries = new HashSet<>();
    for (Integer aggrIndex : aggrIndexes)
      if (remainingAggrIndexesInDevice.contains(aggrIndex))
        remainingAggrIndexesInSeries.add(aggrIndex);
    return remainingAggrIndexesInSeries;
  }

  /**
   * construct an array of timestamps for one batch of a group by partition calculating.
   *
   * @param timestampArray timestamp array
   * @return time array size
   */
  @SuppressWarnings("squid:S3776")
  private int constructTimeArrayForOneCal(
      int iteration, LinkedList<Long> cachedTimestamps, long[] timestampArray) throws IOException {
    int timeArrayLength = 0;
    boolean fromCached = true;
    TimeGenerator timestampGenerator = timestampGenerators.get(iteration);

    for (int cnt = 1;
        cnt < timeStampFetchSize - 1
            && (!cachedTimestamps.isEmpty() || timestampGenerator.hasNext());
        cnt++) {
      if (!cachedTimestamps.isEmpty()) {
        lastTimestamp = cachedTimestamps.remove();
      } else {
        lastTimestamp = timestampGenerator.next();
        fromCached = false;
      }
      if ((groupByTimePlan.isAscending() && lastTimestamp < curEndTime)
          || (!groupByTimePlan.isAscending() && lastTimestamp >= curStartTime)) {
        timestampArray[timeArrayLength++] = lastTimestamp;
      } else {
        // may lastTimestamp get from cache
        if (fromCached) {
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
// group by with VF
