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
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;

/**
 * <p>
 * Manage all series reader in a query as a query node, cooperate with coordinator node for a client
 * query
 * </p>
 */
public interface IClusterLocalSingleQueryManager {

  /**
   * Initially create corresponding series readers.
   */
  void createSeriesReader(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException, PathErrorException, FileNodeManagerException, ProcessorException, QueryFilterOptimizationException;

  /**
   * <p>
   * Read batch data If query round in cache is equal to target query round, it means that batch
   * data in query node transfer to coordinator fail and return cached batch data.
   * </p>
   *
   * @param request request of querying series data
   * @param response response of querying series data
   */
  void readBatchData(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException;

  /**
   * Read batch data of select paths by timestamp
   */
  void readBatchDataByTimestamp(QuerySeriesDataByTimestampRequest request,
      QuerySeriesDataByTimestampResponse response) throws IOException;

  /**
   * Release query resource
   */
  void close() throws FileNodeManagerException;
}
