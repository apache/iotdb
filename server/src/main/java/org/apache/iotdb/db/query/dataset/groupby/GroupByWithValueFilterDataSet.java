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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class GroupByWithValueFilterDataSet extends GroupByEngineDataSet {

  private List<IReaderByTimestamp> allDataReaderList;
  private GroupByPlan groupByPlan;
  private TimeGenerator timestampGenerator;
  /**
   * cached timestamp for next group by partition.
   */
  private long timestamp;
  /**
   * if this object has cached timestamp for next group by partition.
   */
  private boolean hasCachedTimestamp;

  /**
   * group by batch calculation size.
   */
  private int timeStampFetchSize;

  /**
   * constructor.
   */
  public GroupByWithValueFilterDataSet(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    super(context, groupByPlan);
    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
    initGroupBy(context, groupByPlan);
  }

  public GroupByWithValueFilterDataSet(long queryId, GroupByPlan groupByPlan) {
    super(new QueryContext(queryId), groupByPlan);
    this.allDataReaderList = new ArrayList<>();
    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
  }

  /**
   * init reader and aggregate function.
   */
  private void initGroupBy(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    this.timestampGenerator = new ServerTimeGenerator(groupByPlan.getExpression(), context);
    this.allDataReaderList = new ArrayList<>();
    this.groupByPlan = groupByPlan;
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      allDataReaderList.add(new SeriesReaderByTimestamp(path, dataTypes.get(i), context,
          QueryResourceManager.getInstance().getQueryDataSource(path, context, null), null));
    }
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException("need to call hasNext() before calling next()"
          + " in GroupByWithoutValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    List<AggregateResult> aggregateResultList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      aggregateResultList.add(AggregateResultFactory.getAggrResultByName(
          groupByPlan.getDeduplicatedAggregations().get(i),
          groupByPlan.getDeduplicatedDataTypes().get(i)));
    }

    long[] timestampArray = new long[timeStampFetchSize];
    int timeArrayLength = 0;
    if (hasCachedTimestamp) {
      if (timestamp < curEndTime) {
        if (timestamp >= curStartTime) {
          hasCachedTimestamp = false;
          timestampArray[timeArrayLength++] = timestamp;
        }
      } else {
        return constructRowRecord(aggregateResultList);
      }
    }
    while (timestampGenerator.hasNext()) {
      // construct timestamp array
      timeArrayLength = constructTimeArrayForOneCal(timestampArray, timeArrayLength);

      // cal result using timestamp array
      for (int i = 0; i < paths.size(); i++) {
        aggregateResultList.get(i).updateResultUsingTimestamps(
            timestampArray, timeArrayLength, allDataReaderList.get(i));
      }

      timeArrayLength = 0;
      // judge if it's end
      if (timestamp >= curEndTime) {
        hasCachedTimestamp = true;
        break;
      }
    }

    if (timeArrayLength > 0) {
      // cal result using timestamp array
      for (int i = 0; i < paths.size(); i++) {
        aggregateResultList.get(i).updateResultUsingTimestamps(
            timestampArray, timeArrayLength, allDataReaderList.get(i));
      }
    }
    return constructRowRecord(aggregateResultList);
  }

  /**
   * construct an array of timestamps for one batch of a group by partition calculating.
   *
   * @param timestampArray  timestamp array
   * @param timeArrayLength the current size of timestamp array
   * @return time array size
   */
  private int constructTimeArrayForOneCal(long[] timestampArray, int timeArrayLength)
      throws IOException {
    for (int cnt = 1; cnt < timeStampFetchSize && timestampGenerator.hasNext(); cnt++) {
      timestamp = timestampGenerator.next();
      if (timestamp < curEndTime) {
        timestampArray[timeArrayLength++] = timestamp;
      } else {
        hasCachedTimestamp = true;
        break;
      }
    }
    return timeArrayLength;
  }

  private RowRecord constructRowRecord(List<AggregateResult> aggregateResultList) {
    RowRecord record = new RowRecord(curStartTime);
    for (int i = 0; i < paths.size(); i++) {
      AggregateResult aggregateResult = aggregateResultList.get(i);
      record.addField(aggregateResult.getResult(), aggregateResult.getResultDataType());
    }
    return record;
  }
}
