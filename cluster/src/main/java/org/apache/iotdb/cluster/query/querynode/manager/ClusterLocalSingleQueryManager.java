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
package org.apache.iotdb.cluster.query.querynode.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
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

public class ClusterLocalSingleQueryManager {

  private long jobId;

  /**
   * Key is series full path, value is reader of series
   */
  private Map<String, IPointReader> seriesReaders = new HashMap<>();

  /**
   * Key is series full path, value is data type of series
   */
  private Map<String, TSDataType> dataTypeMap = new HashMap<>();

  private QueryProcessExecutor queryProcessExecutor = new OverflowQPExecutor();

  public ClusterLocalSingleQueryManager(long jobId) {
    this.jobId = jobId;
  }

  /**
   * Init create series reader.
   */
  public void createSeriesReader(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException, PathErrorException, FileNodeManagerException, ProcessorException, QueryFilterOptimizationException {
    List<PhysicalPlan> plans = request.getPhysicalPlans();
    for (PhysicalPlan plan : plans) {
      if (plan instanceof GroupByPlan) {
        throw new UnsupportedOperationException();
      } else if (plan instanceof AggregationPlan) {
        throw new UnsupportedOperationException();
      } else {
        QueryContext context = new QueryContext(jobId);
        if (((QueryPlan) plan).getExpression() == null
            || ((QueryPlan) plan).getExpression().getType() == ExpressionType.GLOBAL_TIME) {
          handleDataSetWithoutTimeGenerator((QueryPlan) plan, context, request, response);
        } else {
          throw new UnsupportedOperationException();
        }

      }
    }
  }

  private void handleDataSetWithoutTimeGenerator(QueryPlan plan, QueryContext context,
      QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws PathErrorException, QueryFilterOptimizationException, FileNodeManagerException, ProcessorException, IOException {
    EngineDataSetWithoutTimeGenerator queryDataSet = (EngineDataSetWithoutTimeGenerator) queryProcessExecutor
        .processQuery(plan, context);
    List<Path> paths = plan.getPaths();
    List<IPointReader> readers = queryDataSet.getReaders();
    List<TSDataType> dataTypes = queryDataSet.getDataTypes();
    for (int i = 0; i < paths.size(); i++) {
      String fullPath = paths.get(i).getFullPath();
      IPointReader reader = readers.get(i);
      seriesReaders.put(fullPath, reader);
      dataTypeMap.put(fullPath, dataTypes.get(i));
    }
    response.setSeriesType(dataTypes);
    readBatchData(request, response);
  }

  /**
   * Read batch data
   */
  public void readBatchData(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException {
    List<String> paths = request.getPaths();
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
    response.setSeriesBatchData(batchDataList);
  }

  /**
   * Release query resource
   */
  public void close() throws FileNodeManagerException {
    QueryResourceManager.getInstance().endQueryForGivenJob(jobId);
  }
}
