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
import org.apache.iotdb.cluster.rpc.raft.request.querydata.InitSeriesReaderRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.InitSeriesReaderResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;

/**
 * Manage all local query resources which provide data for coordinator node in cluster query node.
 */
public interface IClusterLocalQueryManager {

  /**
   * Initially create query data set for coordinator node.
   *
   * @param request request for query data from coordinator node
   */
  InitSeriesReaderResponse createQueryDataSet(InitSeriesReaderRequest request)
      throws IOException, FileNodeManagerException, PathErrorException, ProcessorException, QueryFilterOptimizationException;

  /**
   * Read batch data of all querying series in request and set response.
   *
   * @param request request of querying series
   */
  QuerySeriesDataResponse readBatchData(QuerySeriesDataRequest request)
      throws IOException;

  /**
   * Read batch data of select series by batch timestamp which is used in query with value filter
   *  @param request request of querying select paths
   *
   */
  QuerySeriesDataByTimestampResponse readBatchDataByTimestamp(
      QuerySeriesDataByTimestampRequest request) throws IOException;

  /**
   * Close query resource of a task
   *
   * @param taskId task id of local single query manager
   */
  void close(String taskId) throws FileNodeManagerException;


  /**
   * Get query manager by taskId
   *
   * @param taskId task id assigned by ClusterRpcQueryManager
   */
  ClusterLocalSingleQueryManager getSingleQuery(String taskId);

}
