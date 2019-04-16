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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;

public class ClusterLocalQueryManager {

  /**
   * Key is job id, value is manager of a client query.
   */
  ConcurrentHashMap<Long, ClusterLocalSingleQueryManager> singleQueryManagerMap = new ConcurrentHashMap<>();

  private ClusterLocalQueryManager() {
  }

  public void createQueryDataSet(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException, FileNodeManagerException, PathErrorException, ProcessorException, QueryFilterOptimizationException {
    long jobId = QueryResourceManager.getInstance().assignJobId();
    response.setJobId(jobId);
    ClusterLocalSingleQueryManager localQueryManager = new ClusterLocalSingleQueryManager(jobId);
    localQueryManager.init(request, response);
    singleQueryManagerMap.put(jobId, localQueryManager);
  }

  public void readBatchData(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException {
    long jobId = request.getJobId();
    singleQueryManagerMap.get(jobId).readBatchData(request, response);
  }

  public void close(long jobId) throws FileNodeManagerException {
    if(singleQueryManagerMap.containsKey(jobId)){
      singleQueryManagerMap.remove(jobId).close();
    }
  }

  public static final ClusterLocalQueryManager getInstance() {
    return ClusterLocalQueryManager.ClusterLocalQueryManagerHolder.INSTANCE;
  }

  private static class ClusterLocalQueryManagerHolder {

    private static final ClusterLocalQueryManager INSTANCE = new ClusterLocalQueryManager();

    private ClusterLocalQueryManagerHolder() {

    }
  }
}
