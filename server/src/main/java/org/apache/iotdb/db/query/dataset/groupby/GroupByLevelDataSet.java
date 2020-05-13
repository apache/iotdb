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

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_COLUMN;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_COUNT;

public class GroupByLevelDataSet extends QueryDataSet {

  private static final Logger logger = LoggerFactory
    .getLogger(GroupByLevelDataSet.class);

  private List<RowRecord> records = new ArrayList<>();
  private int index = 0;

  protected long queryId;
  private GroupByTimePlan groupByTimePlan;
  private QueryContext context;

  private Map<Path, GroupByExecutor> pathExecutors = new HashMap<>();
  private Map<Path, List<Integer>> resultIndexes = new HashMap<>();

  public GroupByLevelDataSet(QueryContext context, GroupByTimePlan plan, GroupByEngineDataSet dataSet)
    throws QueryProcessException, StorageEngineException, IOException {
    this.queryId = context.getQueryId();
    this.paths = plan.getPaths();
    this.dataTypes = plan.getDataTypes();
    this.groupByTimePlan = plan;
    this.context = context;

    if (logger.isDebugEnabled()) {
      logger.debug("paths " + this.paths + " level:" + plan.getLevel());
    }

    Map<Integer, String> pathIndex = new HashMap<>();
    Map<String, Long> finalPaths = FilePathUtils.getPathByLevel(plan.getPaths(), plan.getLevel(), pathIndex);

    if (!plan.isByTime()) {
      // does not has time interval,
      // so we could group by time interval [MIN_VALUE, MAX_VALUE] to get the total number
      initGroupByLevel();
      RowRecord record = mergeRecordByPath(getRecordWithoutTimeInterval(), finalPaths, pathIndex);
      if (record != null) {
        records.add(record);
      }
    } else {
      // get all records from GroupByDataSet, then we merge every record
      if (logger.isDebugEnabled()) {
        logger.debug("only group by level, paths:" + groupByTimePlan.getPaths());
      }
      while (dataSet != null && dataSet.hasNextWithoutConstraint()) {
        RowRecord curRecord = mergeRecordByPath(dataSet.nextWithoutConstraint(), finalPaths, pathIndex);
        if (curRecord != null) {
          records.add(curRecord);
        }
      }
    }

    this.dataTypes = new ArrayList<>();
    this.paths = new ArrayList<>();
    for (int i = 0; i < finalPaths.size(); i++) {
      this.dataTypes.add(TSDataType.INT64);
    }
  }

  @Override
  protected boolean hasNextWithoutConstraint() throws IOException {
    return index < records.size();
  }

  @Override
  protected RowRecord nextWithoutConstraint() {
    return records.get(index++);
  }

  private void initGroupByLevel()
    throws QueryProcessException, StorageEngineException {
    // get all aggregation results, then we package them to one record
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      if (!pathExecutors.containsKey(path)) {
        //init GroupByExecutor
        pathExecutors.put(path,
          getGroupByExecutor(path, groupByTimePlan.getAllMeasurementsInDevice(path.getDevice()), dataTypes.get(i), this.context, null, null));
        resultIndexes.put(path, new ArrayList<>());
      } else {
        throw new QueryProcessException("duplicated path found, path:" + path);
      }
      resultIndexes.get(path).add(i);
      AggregateResult aggrResult = AggregateResultFactory
        .getAggrResultByName(groupByTimePlan.getDeduplicatedAggregations().get(i), dataTypes.get(i));
      pathExecutors.get(path).addAggregateResult(aggrResult);
    }
  }

  private GroupByExecutor getGroupByExecutor(Path path, Set<String> allSensors, TSDataType dataType,
                                             QueryContext context, Filter timeFilter, TsFileFilter fileFilter)
    throws StorageEngineException, QueryProcessException {
    return new LocalGroupByExecutor(path, allSensors, dataType, context, timeFilter, fileFilter);
  }

  private RowRecord getRecordWithoutTimeInterval()
    throws IOException {
    RowRecord record = new RowRecord(0);
    AggregateResult[] fields = new AggregateResult[paths.size()];

    try {
      for (Map.Entry<Path, GroupByExecutor> pathToExecutorEntry : pathExecutors.entrySet()) {
        GroupByExecutor executor = pathToExecutorEntry.getValue();
        List<AggregateResult> aggregations = executor.calcResult(Long.MIN_VALUE, Long.MAX_VALUE);
        for (int i = 0; i < aggregations.size(); i++) {
          int resultIndex = resultIndexes.get(pathToExecutorEntry.getKey()).get(i);
          fields[resultIndex] = aggregations.get(i);
        }
      }
    } catch (QueryProcessException e) {
      logger.error("GroupByWithoutValueFilterDataSet execute has error", e);
      throw new IOException(e.getMessage(), e);
    }

    for (AggregateResult res : fields) {
      if (res == null) {
        record.addField(null);
        continue;
      }
      record.addField(res.getResult(), res.getResultDataType());
    }
    return record;
  }

  /**
   * merge the raw record by level, for example
   * raw record [timestamp, root.sg1.d1.s0, root.sg1.d1.s1, root.sg1.d2.s2], level=1
   *  and newRecord data is [100, 1, 2]
   * return [100, 3]
   * @param newRecord
   * @param finalPaths
   * @param pathIndex
   * @return
   */
  private RowRecord mergeRecordByPath(RowRecord newRecord,
                                      Map<String, Long> finalPaths,
                                      Map<Integer, String> pathIndex) {
    if (paths.size() != newRecord.getFields().size()) {
      logger.error("bad record, result size not equal path size");
      return null;
    }

    // reset final paths
    for (Map.Entry<String, Long> entry : finalPaths.entrySet()) {
      entry.setValue(0L);
    }

    RowRecord tmpRecord = new RowRecord(newRecord.getTimestamp());

    for (int i = 0; i < newRecord.getFields().size(); i++) {
      if (newRecord.getFields().get(i) != null) {
        finalPaths.put(pathIndex.get(i),
          finalPaths.get(pathIndex.get(i)) + newRecord.getFields().get(i).getLongV());
      }
    }

    for (Map.Entry<String, Long> entry : finalPaths.entrySet()) {
      tmpRecord.addField(Field.getField(entry.getValue(), TSDataType.INT64));
    }

    return tmpRecord;
  }

}
