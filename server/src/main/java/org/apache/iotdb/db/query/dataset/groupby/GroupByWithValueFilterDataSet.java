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
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.layoutoptimize.LayoutNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.layoutoptimize.layoutholder.LayoutHolder;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class GroupByWithValueFilterDataSet extends GroupByEngineDataSet {

  private List<IReaderByTimestamp> allDataReaderList;
  private GroupByTimePlan groupByTimePlan;
  private TimeGenerator timestampGenerator;
  /** cached timestamp for next group by partition. */
  private LinkedList<Long> cachedTimestamps = new LinkedList<>();
  /** group by batch calculation size. */
  protected int timeStampFetchSize;

  private Map<Integer, Integer> resultToPathIdx = new HashMap<>();
  private List<Path> pathsInPhysicalOrder = new ArrayList<>();

  private long lastTimestamp;

  protected GroupByWithValueFilterDataSet() {}

  /** constructor. */
  public GroupByWithValueFilterDataSet(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {
    super(context, groupByTimePlan);
    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
    initGroupBy(context, groupByTimePlan);
  }

  @TestOnly
  public GroupByWithValueFilterDataSet(long queryId, GroupByTimePlan groupByTimePlan) {
    super(new QueryContext(queryId), groupByTimePlan);
    this.allDataReaderList = new ArrayList<>();
    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
  }

  /** init reader and aggregate function. */
  protected void initGroupBy(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {
    this.timestampGenerator = getTimeGenerator(context, groupByTimePlan);
    this.allDataReaderList = new ArrayList<>();
    this.groupByTimePlan = groupByTimePlan;

    List<StorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLock(paths.stream().map(p -> (PartialPath) p).collect(Collectors.toList()));
    WorkloadManager manager = WorkloadManager.getInstance();
    Map<String, List<Integer>> deviceQueryIdxMap = new HashMap<>();
    try {
      for (int i = 0; i < paths.size(); i++) {
        PartialPath path = (PartialPath) paths.get(i);
        allDataReaderList.add(
            getReaderByTime(path, groupByTimePlan, dataTypes.get(i), context, null));
        // record the map: device -> path idx
        if (!deviceQueryIdxMap.containsKey(path.getDevice())) {
          deviceQueryIdxMap.put(path.getDevice(), new ArrayList<>());
        }
        deviceQueryIdxMap.get(path.getDevice()).add(i);
      }

      // add the record to the workload manager
      for (String deviceID : deviceQueryIdxMap.keySet()) {
        List<Integer> pathIndexes = deviceQueryIdxMap.get(deviceID);
        List<String> measurements = new ArrayList<>();
        for (int idx : pathIndexes) {
          PartialPath path = (PartialPath) paths.get(idx);
          measurements.add(path.getMeasurement());
        }
        manager.addQueryRecord(
            deviceID, measurements, groupByTimePlan.getEndTime() - groupByTimePlan.getStartTime());
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    return new ServerTimeGenerator(context, queryPlan);
  }

  protected IReaderByTimestamp getReaderByTime(
      PartialPath path,
      RawDataQueryPlan queryPlan,
      TSDataType dataType,
      QueryContext context,
      TsFileFilter fileFilter)
      throws StorageEngineException, QueryProcessException {
    return new SeriesReaderByTimestamp(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null),
        fileFilter,
        ascending);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next()" + " in GroupByWithoutValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    if (pathsInPhysicalOrder.size() == 0) {
      Map<String, Set<Path>> pathForEachDevice = new HashMap<>();
      Map<Path, Integer> pathToIndex = new HashMap<>();
      for (int i = 0; i < paths.size(); ++i) {
        Path path = paths.get(i);
        pathToIndex.put(path, i);
        if (!pathForEachDevice.containsKey(path.getDevice())) {
          pathForEachDevice.put(path.getDevice(), new HashSet<>());
        }
        pathForEachDevice.get(path.getDevice()).add(path);
      }
      LayoutHolder holder = LayoutHolder.getInstance();
      for (String device : pathForEachDevice.keySet()) {
        if (!holder.hasLayoutForDevice(device)) {
          holder.updateMetadata();
        }
        Set<Path> pathsForCurDevice = pathForEachDevice.get(device);
        try {
          List<String> measurements = holder.getMeasurementForDevice(device);
          if (measurements.size() < pathForEachDevice.size()) {
            holder.updateMetadata();
            measurements = holder.getMeasurementForDevice(device);
          }
          for (String measurement : measurements) {
            Path path = new Path(device, measurement);
            if (pathsForCurDevice.contains(path)) {
              pathsInPhysicalOrder.add(path);
              resultToPathIdx.put(pathsInPhysicalOrder.size() - 1, pathToIndex.get(path));
            }
          }
        } catch (LayoutNotExistException e) {
          for (Path path : pathsForCurDevice) {
            pathsInPhysicalOrder.add(path);
            resultToPathIdx.put(pathsInPhysicalOrder.size() - 1, pathToIndex.get(path));
          }
        }
      }
    }
    List<AggregateResult> aggregateResultList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      aggregateResultList.add(
          AggregateResultFactory.getAggrResultByName(
              groupByTimePlan.getDeduplicatedAggregations().get(i),
              groupByTimePlan.getDeduplicatedDataTypes().get(i),
              ascending));
    }

    long[] timestampArray = new long[timeStampFetchSize];
    int timeArrayLength = 0;

    if (!cachedTimestamps.isEmpty()) {
      long timestamp = cachedTimestamps.remove();
      if (timestamp < curEndTime) {
        if (!groupByTimePlan.isAscending() && timestamp < curStartTime) {
          cachedTimestamps.addFirst(timestamp);
          return constructRowRecord(aggregateResultList);
        }
        if (timestamp >= curStartTime) {
          timestampArray[timeArrayLength++] = timestamp;
        }
      } else {
        cachedTimestamps.addFirst(timestamp);
        return constructRowRecord(aggregateResultList);
      }
    }

    while (!cachedTimestamps.isEmpty() || timestampGenerator.hasNext()) {
      // construct timestamp array
      timeArrayLength = constructTimeArrayForOneCal(timestampArray, timeArrayLength);

      // cal result using timestamp array
      for (int i = 0; i < pathsInPhysicalOrder.size(); i++) {
        int idx = resultToPathIdx.get(i);
        aggregateResultList
            .get(idx)
            .updateResultUsingTimestamps(
                timestampArray, timeArrayLength, allDataReaderList.get(idx));
      }

      timeArrayLength = 0;
      // judge if it's end
      if ((groupByTimePlan.isAscending() && lastTimestamp >= curEndTime)
          || (!groupByTimePlan.isAscending() && lastTimestamp < curStartTime)) {
        break;
      }
    }

    if (timeArrayLength > 0) {
      // cal result using timestamp array
      for (int i = 0; i < paths.size(); i++) {
        aggregateResultList
            .get(i)
            .updateResultUsingTimestamps(timestampArray, timeArrayLength, allDataReaderList.get(i));
      }
    }
    return constructRowRecord(aggregateResultList);
  }

  @Override
  @SuppressWarnings("squid:S3776")
  public Pair<Long, Object> peekNextNotNullValue(Path path, int i) throws IOException {
    if ((!timestampGenerator.hasNext() && cachedTimestamps.isEmpty())
        || allDataReaderList.get(i).readerIsEmpty()) {
      return null;
    }

    long[] timestampArray = new long[1];
    AggregateResult aggrResultByName =
        AggregateResultFactory.getAggrResultByName(
            groupByTimePlan.getDeduplicatedAggregations().get(i),
            groupByTimePlan.getDeduplicatedDataTypes().get(i),
            ascending);

    long tmpStartTime = curStartTime - slidingStep;
    int index = 0;
    while (tmpStartTime >= startTime
        && (timestampGenerator.hasNext() || !cachedTimestamps.isEmpty())) {
      long timestamp = Long.MIN_VALUE;
      if (timestampGenerator.hasNext()) {
        cachedTimestamps.add(timestampGenerator.next());
      }
      if (!cachedTimestamps.isEmpty() && index < cachedTimestamps.size()) {
        timestamp = cachedTimestamps.get(index++);
      }
      if (timestamp >= tmpStartTime) {
        timestampArray[0] = timestamp;
      } else {
        do {
          tmpStartTime -= slidingStep;
          if (timestamp >= tmpStartTime) {
            timestampArray[0] = timestamp;
            break;
          }
        } while (tmpStartTime >= startTime);
      }
      aggrResultByName.updateResultUsingTimestamps(timestampArray, 1, allDataReaderList.get(i));

      if (aggrResultByName.getResult() != null) {
        return new Pair<>(tmpStartTime, aggrResultByName.getResult());
      }
    }
    return null;
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

  private RowRecord constructRowRecord(List<AggregateResult> aggregateResultList) {
    RowRecord record;
    if (leftCRightO) {
      record = new RowRecord(curStartTime);
    } else {
      record = new RowRecord(curEndTime - 1);
    }
    for (int i = 0; i < paths.size(); i++) {
      AggregateResult aggregateResult = aggregateResultList.get(i);
      record.addField(aggregateResult.getResult(), aggregateResult.getResultDataType());
    }
    return record;
  }
}
