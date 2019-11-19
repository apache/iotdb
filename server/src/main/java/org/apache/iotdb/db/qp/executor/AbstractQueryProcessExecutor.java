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
package org.apache.iotdb.db.qp.executor;

import static org.apache.iotdb.db.conf.IoTDBConstant.ITEM;
import static org.apache.iotdb.db.conf.IoTDBConstant.PARAMETER;
import static org.apache.iotdb.db.conf.IoTDBConstant.STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.TTL;
import static org.apache.iotdb.db.conf.IoTDBConstant.VALUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.engine.flush.pool.FlushTaskPoolManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.DeviceIterateDataSet;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.query.executor.IEngineQueryRouter;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class AbstractQueryProcessExecutor implements IQueryProcessExecutor {

  IEngineQueryRouter queryRouter = new EngineQueryRouter();

  @Override
  public QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException, QueryFilterOptimizationException, QueryProcessException {
    if (queryPlan instanceof QueryPlan) {
      return processDataQuery((QueryPlan) queryPlan, context);
    } else if (queryPlan instanceof AuthorPlan) {
      return processAuthorQuery((AuthorPlan) queryPlan, context);
    } else if (queryPlan instanceof ShowPlan) {
      return processShowQuery((ShowPlan) queryPlan);
    } else {
      throw new QueryProcessException(String.format("Unrecognized query plan %s", queryPlan));
    }
  }

  private QueryDataSet processShowQuery(ShowPlan showPlan) throws QueryProcessException {
    switch (showPlan.getShowContentType()) {
      case TTL:
        return processShowTTLQuery((ShowTTLPlan) showPlan);
      case DYNAMIC_PARAMETER:
        return processShowDynamicParameterQuery();
      case FLUSH_TASK_INFO:
        return processShowFlushTaskInfo();
      default:
        throw new QueryProcessException(String.format("Unrecognized show plan %s", showPlan));
    }
  }

  private QueryDataSet processShowTTLQuery(ShowTTLPlan showTTLPlan) {
    List<Path> paths = new ArrayList<>();
    paths.add(new Path(STORAGE_GROUP));
    paths.add(new Path(TTL));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.TEXT);
    dataTypes.add(TSDataType.INT64);
    ListDataSet listDataSet = new ListDataSet(paths, dataTypes);

    List<String> selectedSgs = showTTLPlan.getStorageGroups();

    List<MNode> storageGroups = MManager.getInstance().getAllStorageGroups();
    int timestamp = 0;
    for (MNode mNode : storageGroups) {
      String sgName = mNode.getFullPath();
      if (!selectedSgs.isEmpty() && !selectedSgs.contains(sgName)) {
        continue;
      }
      RowRecord rowRecord = new RowRecord(timestamp++);
      Field sg = new Field(TSDataType.TEXT);
      Field ttl;
      sg.setBinaryV(new Binary(sgName));
      if (mNode.getDataTTL() != Long.MAX_VALUE) {
        ttl = new Field(TSDataType.INT64);
        ttl.setLongV(mNode.getDataTTL());
      } else {
        ttl = new Field(null);
      }
      rowRecord.addField(sg);
      rowRecord.addField(ttl);
      listDataSet.putRecord(rowRecord);
    }

    return listDataSet;
  }

  private QueryDataSet processShowDynamicParameterQuery() {
    List<Path> paths = new ArrayList<>();
    paths.add(new Path(PARAMETER));
    paths.add(new Path(VALUE));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.TEXT);
    dataTypes.add(TSDataType.TEXT);
    ListDataSet listDataSet = new ListDataSet(paths, dataTypes);

    int timestamp = 0;
    addRowRecordForShowQuery(listDataSet, timestamp++, "memtable size threshold",
        IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold() + "B");
    addRowRecordForShowQuery(listDataSet, timestamp++, "memtable number",
        IoTDBDescriptor.getInstance().getConfig().getMaxMemtableNumber() + "B");
    addRowRecordForShowQuery(listDataSet, timestamp++, "tsfile size threshold",
        IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold() + "B");
    addRowRecordForShowQuery(listDataSet, timestamp++, "compression ratio",
        Double.toString(CompressionRatio.getInstance().getRatio()));
    addRowRecordForShowQuery(listDataSet, timestamp++, "storage group number",
        Integer.toString( MManager.getInstance().getAllStorageGroupNames().size()));
    addRowRecordForShowQuery(listDataSet, timestamp++, "timeseries number",
        Integer.toString(IoTDBConfigDynamicAdapter.getInstance().getTotalTimeseries()));
    addRowRecordForShowQuery(listDataSet, timestamp++,
        "maximal timeseries number among storage groups",
        Long.toString(MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups()));
    return listDataSet;
  }

  private QueryDataSet processShowFlushTaskInfo() {
    List<Path> paths = new ArrayList<>();
    paths.add(new Path(ITEM));
    paths.add(new Path(VALUE));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.TEXT);
    dataTypes.add(TSDataType.TEXT);
    ListDataSet listDataSet = new ListDataSet(paths, dataTypes);

    int timestamp = 0;
    addRowRecordForShowQuery(listDataSet, timestamp++, "total number of flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getTotalTasks()));
    addRowRecordForShowQuery(listDataSet, timestamp++, "number of working flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getWorkingTasksNumber()));
    addRowRecordForShowQuery(listDataSet, timestamp++, "number of waiting flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getWaitingTasksNumber()));
    return listDataSet;
  }

  private void addRowRecordForShowQuery(ListDataSet listDataSet, int timestamp, String item,
      String value) {
    RowRecord rowRecord = new RowRecord(timestamp);
    Field itemField = new Field(TSDataType.TEXT);
    itemField.setBinaryV(new Binary(item));
    Field valueField = new Field(TSDataType.TEXT);
    valueField.setBinaryV(new Binary(value));
    rowRecord.addField(itemField);
    rowRecord.addField(valueField);
    listDataSet.putRecord(rowRecord);
  }

  protected abstract QueryDataSet processAuthorQuery(AuthorPlan plan, QueryContext context)
      throws QueryProcessException;

  private QueryDataSet processDataQuery(QueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryFilterOptimizationException, QueryProcessException,
      IOException {
    if (queryPlan.isGroupByDevice()) {
      return new DeviceIterateDataSet(queryPlan, context, queryRouter);
    }

    // deduplicate executed paths and aggregations if exist
    List<Path> deduplicatedPaths = new ArrayList<>();

    if (queryPlan instanceof GroupByPlan) {
      GroupByPlan groupByPlan = (GroupByPlan) queryPlan;
      List<String> deduplicatedAggregations = new ArrayList<>();
      deduplicate(groupByPlan.getPaths(), groupByPlan.getAggregations(), deduplicatedPaths,
          deduplicatedAggregations);
      return groupBy(deduplicatedPaths, deduplicatedAggregations, groupByPlan.getExpression(),
          groupByPlan.getUnit(),
          groupByPlan.getOrigin(), groupByPlan.getIntervals(), context);
    }

    if (queryPlan instanceof AggregationPlan) {
      List<String> deduplicatedAggregations = new ArrayList<>();
      deduplicate(queryPlan.getPaths(), queryPlan.getAggregations(), deduplicatedPaths,
          deduplicatedAggregations);
      return aggregate(deduplicatedPaths, deduplicatedAggregations, queryPlan.getExpression(),
          context);
    }

    if (queryPlan instanceof FillQueryPlan) {
      FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
      deduplicate(queryPlan.getPaths(), deduplicatedPaths);
      return fill(deduplicatedPaths, fillQueryPlan.getQueryTime(), fillQueryPlan.getFillType(),
          context);
    }

    deduplicate(queryPlan.getPaths(), deduplicatedPaths);
    QueryExpression queryExpression = QueryExpression.create().setSelectSeries(deduplicatedPaths)
        .setExpression(queryPlan.getExpression());
    return queryRouter.query(queryExpression, context);
  }

  /**
   * Note that the deduplication strategy must be consistent with that of IoTDBQueryResultSet.
   */
  private void deduplicate(List<Path> paths, List<String> aggregations,
      List<Path> deduplicatedPaths,
      List<String> deduplicatedAggregations) throws QueryProcessException {
    if (paths == null || aggregations == null || deduplicatedPaths == null
        || deduplicatedAggregations == null) {
      throw new QueryProcessException("Parameters should not be null.");
    }
    if (paths.size() != aggregations.size()) {
      throw new QueryProcessException(
          "The size of the path list does not equal that of the aggregation list.");
    }
    Set<String> columnSet = new HashSet<>();
    for (int i = 0; i < paths.size(); i++) {
      String column = aggregations.get(i) + "(" + paths.get(i).toString() + ")";
      if (!columnSet.contains(column)) {
        deduplicatedPaths.add(paths.get(i));
        deduplicatedAggregations.add(aggregations.get(i));
        columnSet.add(column);
      }
    }
  }

  /**
   * Note that the deduplication strategy must be consistent with that of IoTDBQueryResultSet.
   */
  private void deduplicate(List<Path> paths, List<Path> deduplicatedPaths)
      throws QueryProcessException {
    if (paths == null || deduplicatedPaths == null) {
      throw new QueryProcessException("Parameters should not be null.");
    }
    Set<String> columnSet = new HashSet<>();
    for (Path path : paths) {
      String column = path.toString();
      if (!columnSet.contains(column)) {
        deduplicatedPaths.add(path);
        columnSet.add(column);
      }
    }
  }

  @Override
  public void delete(DeletePlan deletePlan) throws QueryProcessException {
    try {
      MManager mManager = MManager.getInstance();
      Set<String> existingPaths = new HashSet<>();
      for (Path p : deletePlan.getPaths()) {
        existingPaths.addAll(mManager.getPaths(p.getFullPath()));
      }
      if (existingPaths.isEmpty()) {
        throw new QueryProcessException(
            "TimeSeries does not exist and its data cannot be deleted");
      }
      for (String onePath : existingPaths) {
        if (!mManager.pathExist(onePath)) {
          throw new QueryProcessException(String
              .format("TimeSeries %s does not exist and its data cannot be deleted", onePath));
        }
      }
      for (String path : existingPaths) {
        delete(new Path(path), deletePlan.getDeleteTime());
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
  }
}
