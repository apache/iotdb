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
package org.apache.iotdb.cluster.query.manager.querynode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutTimeGenerator;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;

/**
 * Manage all series reader in a query as a query node, cooperate with coordinator node for a client
 * query
 */
public class ClusterLocalSingleQueryManager {

  /**
   * Job id assigned by local QueryResourceManager
   */
  private long jobId;

  /**
   * Represents the number of query rounds, initial value is -1.
   */
  private long queryRound = -1;

  /**
   * Key is series full path, value is reader of select series
   */
  private Map<String, IPointReader> selectSeriesReaders = new HashMap<>();

  /**
   * Key is series full path, value is reader of filter series
   */
  private Map<String, IPointReader> filterSeriesReaders = new HashMap<>();

  /**
   * Key is series full path, value is data type of series
   */
  private Map<String, TSDataType> dataTypeMap = new HashMap<>();

  /**
   * Cached batch data result
   */
  private List<BatchData> cachedBatchDataResult = new ArrayList<>();

  private QueryProcessExecutor queryProcessExecutor = new OverflowQPExecutor();

  public ClusterLocalSingleQueryManager(long jobId) {
    this.jobId = jobId;
  }

  /**
   * Initially create corresponding series readers.
   */
  public void createSeriesReader(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException, PathErrorException, FileNodeManagerException, ProcessorException, QueryFilterOptimizationException {
    Map<PathType, QueryPlan> queryPlanMap = request.getAllQueryPlan();
    for (Entry<PathType, QueryPlan> entry : queryPlanMap.entrySet()) {
      PathType pathType = entry.getKey();
      QueryPlan plan = entry.getValue();
      if (plan instanceof GroupByPlan) {
        throw new UnsupportedOperationException();
      } else if (plan instanceof AggregationPlan) {
        throw new UnsupportedOperationException();
      } else {
        QueryContext context = new QueryContext(jobId);
        if (plan.getExpression() == null
            || plan.getExpression().getType() == ExpressionType.GLOBAL_TIME) {
          handleDataSetWithoutTimeGenerator(plan, context, response, pathType);
        } else {
          throw new UnsupportedOperationException();
        }
      }
    }
  }

  /**
   * Handle query with no filter or only global time filter
   *
   * @param plan plan query plan
   * @param context query context
   * @param response response for coordinator node
   * @param pathType type of series
   */
  private void handleDataSetWithoutTimeGenerator(QueryPlan plan, QueryContext context,
      QuerySeriesDataResponse response, PathType pathType)
      throws PathErrorException, QueryFilterOptimizationException, FileNodeManagerException, ProcessorException, IOException {
    EngineDataSetWithoutTimeGenerator queryDataSet = (EngineDataSetWithoutTimeGenerator) queryProcessExecutor
        .processQuery(plan, context);
    List<Path> paths = plan.getPaths();
    List<String> series = new ArrayList<>();
    List<IPointReader> readers = queryDataSet.getReaders();
    List<TSDataType> dataTypes = queryDataSet.getDataTypes();
    for (int i = 0; i < paths.size(); i++) {
      String fullPath = paths.get(i).getFullPath();
      IPointReader reader = readers.get(i);
      series.add(fullPath);
      selectSeriesReaders.put(fullPath, reader);
      dataTypeMap.put(fullPath, dataTypes.get(i));
    }
    response.getSeriesDataTypes().put(pathType, dataTypes);
  }

  /**
   * Read batch data If query round in cache is equal to target query round, it means that batch
   * data in query node transfer to coordinator fail and return cached batch data.
   */
  public void readBatchData(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException {
    long targetQueryRounds = request.getQueryRounds();
    if (targetQueryRounds != this.queryRound) {
      this.queryRound = targetQueryRounds;
        PathType pathType = request.getPathType();
        List<String> paths = request.getSeriesPaths();
        List<BatchData> batchDataList;
        if (pathType == PathType.SELECT_PATH) {
          batchDataList = readSingelPathTypeBatchData(paths, selectSeriesReaders);
        } else {
          batchDataList = readSingelPathTypeBatchData(paths, filterSeriesReaders);
        }
        cachedBatchDataResult = batchDataList;
    }
    response.setSeriesBatchData(cachedBatchDataResult);
  }

  /**
   * Read batch data of a path type
   *
   * @param paths all series to query
   * @param seriesReaders Corresponding series reader
   */
  private List<BatchData> readSingelPathTypeBatchData(List<String> paths,
      Map<String, IPointReader> seriesReaders) throws IOException {
    List<BatchData> batchDataList = new ArrayList<>();
    for (String fullPath : paths) {
      BatchData batchData = new BatchData(dataTypeMap.get(fullPath));
      IPointReader reader = seriesReaders.get(fullPath);
      for (int i = 0; i < ClusterConstant.BATCH_READ_SIZE; i++) {
        if (reader.hasNext()) {
          TimeValuePair pair = reader.next();
          batchData.putTime(pair.getTimestamp());
          batchData.putAnObject(pair.getValue().getValue());
        } else {
          break;
        }
      }
      batchDataList.add(batchData);
    }
    return batchDataList;
  }

  /**
   * Release query resource
   */
  public void close() throws FileNodeManagerException {
    QueryResourceManager.getInstance().endQueryForGivenJob(jobId);
  }
}
