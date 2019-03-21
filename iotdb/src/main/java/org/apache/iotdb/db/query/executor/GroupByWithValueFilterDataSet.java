/**
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

package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

public class GroupByWithValueFilterDataSet extends GroupByEngine {


  private List<EngineReaderByTimeStamp> allDataReaderList;
  private TimeGenerator timestampGenerator;
  private long timestamp;
  private boolean hasCachedTimestamp;

  //group by batch calculation size.
  private int timeStampFetchSize;

  /**
   * constructor.
   */
  public GroupByWithValueFilterDataSet(long jobId, List<Path> paths, long unit, long origin,
      List<Pair<Long, Long>> mergedIntervals) {
    super(jobId, paths, unit, origin, mergedIntervals);
    this.allDataReaderList = new ArrayList<>();
    this.timeStampFetchSize = 10 * IoTDBDescriptor.getInstance().getConfig().getFetchSize();
  }

  /**
   * init reader and aggregate function.
   */
  public void initGroupBy(QueryContext context, List<String> aggres, IExpression expression)
      throws FileNodeManagerException, PathErrorException, ProcessorException, IOException {
    initAggreFuction(context, aggres, expression);

    QueryTokenManager.getInstance().beginQueryOfGivenExpression(jobId, expression);
    QueryTokenManager.getInstance().beginQueryOfGivenQueryPaths(jobId, selectedSeries);
    this.timestampGenerator = new EngineTimeGenerator(jobId, expression, context);
    this.allDataReaderList = SeriesReaderFactory
        .getByTimestampReadersOfSelectedPaths(jobId, selectedSeries, context);
  }

  @Override
  public RowRecord next() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next() in GroupByWithOnlyTimeFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    for (AggregateFunction function : functions) {
      function.init();
    }

    List<Long> timestampList = new ArrayList<>(timeStampFetchSize);
    if (hasCachedTimestamp) {
      if (timestamp < endTime) {
        hasCachedTimestamp = false;
        timestampList.add(timestamp);
      } else {
        //所有域均为空
        return constructRowRecord();
      }
    }

    while (timestampGenerator.hasNext()) {
      //construct timestamp list
      for (int cnt = 1; cnt < timeStampFetchSize; cnt++) {
        if (!timestampGenerator.hasNext()) {
          break;
        }
        timestamp = timestampGenerator.next();
        if (timestamp < endTime) {
          timestampList.add(timestamp);
        } else {
          hasCachedTimestamp = true;
          break;
        }
      }

      //cal result using timestamp list
      for (int i = 0; i < selectedSeries.size(); i++) {
        functions.get(i).calcAggregationUsingTimestamps(timestampList, allDataReaderList.get(i));
      }

      timestampList.clear();
      //judge if it's end
      if (timestamp >= endTime) {
        hasCachedTimestamp = true;
        break;
      }
    }

    if(!timestampList.isEmpty()){
      //cal result using timestamp list
      for (int i = 0; i < selectedSeries.size(); i++) {
        functions.get(i).calcAggregationUsingTimestamps(timestampList, allDataReaderList.get(i));
      }
    }
    return constructRowRecord();
  }

  private RowRecord constructRowRecord() {
    RowRecord record = new RowRecord(startTime);
    for (int i = 0; i < functions.size(); i++) {
      record.addField(getField(functions.get(i).getResult()));
    }
    return record;
  }
}
