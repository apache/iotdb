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
package org.apache.iotdb.cluster.rpc.processor;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.rpc.request.QueryTimeSeriesRequest;
import org.apache.iotdb.cluster.rpc.response.QueryTimeSeriesResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;


public class QueryTimeSeriesAsyncProcessor extends BasicAsyncUserProcessor<QueryTimeSeriesRequest> {

  private MManager mManager = MManager.getInstance();

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      QueryTimeSeriesRequest request) {
    String groupId = request.getGroupID();
    final byte[] reqContext = RaftUtils.createRaftRequestContext();
    DataPartitionRaftHolder dataPartitionHolder = RaftUtils.getDataPartitonRaftHolder(groupId);

    if (request.getReadConsistencyLevel() == ClusterConstant.WEAK_CONSISTENCY_LEVEL) {
      QueryTimeSeriesResponse response = QueryTimeSeriesResponse
          .createEmptyInstance(groupId);
      try {
        queryTimeSeries(request, response);
      } catch (final PathErrorException e) {
        response = QueryTimeSeriesResponse.createErrorInstance(groupId, e.getMessage());
      }
      asyncContext.sendResponse(response);
    } else {
      ((RaftService) dataPartitionHolder.getService()).getNode()
          .readIndex(reqContext, new ReadIndexClosure() {

            @Override
            public void run(Status status, long index, byte[] reqCtx) {
              QueryTimeSeriesResponse response = QueryTimeSeriesResponse
                  .createEmptyInstance(groupId);
              if (status.isOk()) {
                try {
                  queryTimeSeries(request, response);
                } catch (final PathErrorException e) {
                  response = QueryTimeSeriesResponse.createErrorInstance(groupId, e.getMessage());
                }
              } else {
                response = QueryTimeSeriesResponse
                    .createErrorInstance(groupId, status.getErrorMsg());
              }
              asyncContext.sendResponse(response);
            }
          });
    }
  }

  /**
   * Query timeseries
   */
  private void queryTimeSeries(QueryTimeSeriesRequest queryMetadataRequest,
      QueryTimeSeriesResponse response) throws PathErrorException {
    for (String path : queryMetadataRequest.getPath()) {
      response.addTimeSeries(mManager.getShowTimeseriesPath(path));
    }
  }

  @Override
  public String interest() {
    return QueryTimeSeriesRequest.class.getName();
  }
}
