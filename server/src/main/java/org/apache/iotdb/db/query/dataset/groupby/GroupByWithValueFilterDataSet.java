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
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.common.TimeColumn;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class GroupByWithValueFilterDataSet extends GroupByEngineDataSet {

  private List<IReaderByTimestamp> allDataReaderList;
  private GroupByPlan groupByPlan;
  private TimeGenerator timestampGenerator;

  private TimeColumn timeColumn = new TimeColumn();

  /**
   * constructor.
   */
  public GroupByWithValueFilterDataSet(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    super(context, groupByPlan);
    initGroupBy(context, groupByPlan);
  }

  public GroupByWithValueFilterDataSet(long queryId, GroupByPlan groupByPlan) {
    super(new QueryContext(queryId), groupByPlan);
    this.allDataReaderList = new ArrayList<>();
  }

  /**
   * init reader and aggregate function.
   */
  private void initGroupBy(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    this.timestampGenerator = getTimeGenerator(groupByPlan.getExpression(), context);
    this.allDataReaderList = new ArrayList<>();
    this.groupByPlan = groupByPlan;
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      allDataReaderList.add(getReaderByTime(path, dataTypes.get(i), context, null));
    }
  }

  private TimeGenerator getTimeGenerator(IExpression expression, QueryContext context)
      throws StorageEngineException {
    return new ServerTimeGenerator(expression, context);
  }

  private IReaderByTimestamp getReaderByTime(Path path, TSDataType dataType, QueryContext context,
      TsFileFilter fileFilter) throws StorageEngineException {
    return new SeriesReaderByTimestamp(path, dataType, context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null), fileFilter);
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

    if (timeColumn != null && timeColumn.hasCurrent()) {
      //skip early time
      while (timeColumn.currentTime() < curStartTime && timeColumn.hasCurrent()) {
        timeColumn.next();
      }
      if (timeColumn.currentTime() >= curEndTime) {
        return constructRowRecord(aggregateResultList);
      }
    }

    while (timestampGenerator.hasNextTimeColumn() || timeColumn.hasCurrent()) {
      if (timeColumn == null || !timeColumn.hasCurrent()) {
        timeColumn = timestampGenerator.nextTimeColumn();
      }
      if (timeColumn.currentTime() >= curEndTime) {
        break;
      }
      int index = timeColumn.position();
      // cal result using timestamp array
      for (int i = 0; i < paths.size(); i++) {
        AggregateResult result = aggregateResultList.get(i);
        if (!result.isCalculatedAggregationResult()) {
          timeColumn.position(index);
          result.updateResultUsingTimestamps(timeColumn, curEndTime, allDataReaderList.get(i));
        }
      }
    }

    return constructRowRecord(aggregateResultList);
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
