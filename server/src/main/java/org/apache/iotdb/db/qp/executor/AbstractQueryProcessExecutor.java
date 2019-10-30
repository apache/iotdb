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

import static org.apache.iotdb.db.conf.IoTDBConstant.STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.TTL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.dataset.DeviceIterateDataSet;
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
      throws IOException, StorageEngineException, PathErrorException,
      QueryFilterOptimizationException, ProcessorException {

    if (queryPlan instanceof QueryPlan) {
      return processDataQuery((QueryPlan) queryPlan, context);
    } else if (queryPlan instanceof AuthorPlan) {
      return processAuthorQuery((AuthorPlan) queryPlan, context);
    } else if(queryPlan instanceof ShowTTLPlan) {
      return processShowTTLQuery((ShowTTLPlan) queryPlan);
    } else {
      throw new ProcessorException(String.format("Unrecognized query plan %s", queryPlan));
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
    int i = 0;
    for (MNode mNode : storageGroups) {
      String sgName = mNode.getFullPath();
      if (!selectedSgs.isEmpty() && !selectedSgs.contains(sgName)) {
        continue;
      }
      RowRecord rowRecord = new RowRecord(i++);
      Field sg = new Field(TSDataType.TEXT);
      Field ttl;
      sg.setBinaryV(new Binary(sgName));
      if (mNode.getDataTTL() != Long.MAX_VALUE) {
        ttl = new Field(TSDataType.INT64);
        ttl.setLongV(mNode.getDataTTL());
      } else {
        ttl = new Field(null);
        ttl.setNull();
      }
      rowRecord.addField(sg);
      rowRecord.addField(ttl);
      listDataSet.putRecord(rowRecord);
    }

    return listDataSet;
  }

  protected abstract QueryDataSet processAuthorQuery(AuthorPlan plan, QueryContext context)
      throws ProcessorException;

  private QueryDataSet processDataQuery(QueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryFilterOptimizationException, PathErrorException,
      ProcessorException, IOException {
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
      List<String> deduplicatedAggregations) throws ProcessorException {
    if (paths == null || aggregations == null || deduplicatedPaths == null
        || deduplicatedAggregations == null) {
      throw new ProcessorException("Parameters should not be null.");
    }
    if (paths.size() != aggregations.size()) {
      throw new ProcessorException(
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
      throws ProcessorException {
    if (paths == null || deduplicatedPaths == null) {
      throw new ProcessorException("Parameters should not be null.");
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
  public boolean delete(DeletePlan deletePlan) throws ProcessorException {
    try {
      boolean result = true;
      MManager mManager = MManager.getInstance();
      Set<String> existingPaths = new HashSet<>();
      for (Path p : deletePlan.getPaths()) {
        existingPaths.addAll(mManager.getPaths(p.getFullPath()));
      }
      if (existingPaths.isEmpty()) {
        throw new ProcessorException("TimeSeries does not exist and its data cannot be deleted");
      }
      for (String onePath : existingPaths) {
        if (!mManager.pathExist(onePath)) {
          throw new ProcessorException(
              String
                  .format("TimeSeries %s does not exist and its data cannot be deleted", onePath));
        }
      }
      for (String path : existingPaths) {
        result &= delete(new Path(path), deletePlan.getDeleteTime());
      }
      return result;
    } catch (MetadataErrorException e) {
      throw new ProcessorException(e);
    }
  }

}
