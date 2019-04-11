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
package org.apache.iotdb.cluster.rpc.raft.processor;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.rpc.raft.request.QuerySeriesTypeRequest;
import org.apache.iotdb.cluster.rpc.raft.response.QuerySeriesTypeResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;

public class QuerySeriesTypeAsyncProcessor extends BasicAsyncUserProcessor<QuerySeriesTypeRequest> {

  private MManager mManager = MManager.getInstance();

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      QuerySeriesTypeRequest request) {
    String groupId = request.getGroupID();

    if (request.getReadConsistencyLevel() == ClusterConstant.WEAK_CONSISTENCY_LEVEL) {
      QuerySeriesTypeResponse response;
      try {
        response = QuerySeriesTypeResponse.createSuccessResponse(groupId, mManager.getSeriesType(request.getPath()));
        response.addResult(true);
      } catch (final PathErrorException e) {
        response = QuerySeriesTypeResponse.createErrorResponse(groupId, e.getMessage());
        response.addResult(false);
      }
      asyncContext.sendResponse(response);
    } else {
      final byte[] reqContext = RaftUtils.createRaftRequestContext();
      DataPartitionRaftHolder dataPartitionHolder = RaftUtils.getDataPartitonRaftHolder(groupId);

      ((RaftService) dataPartitionHolder.getService()).getNode()
          .readIndex(reqContext, new ReadIndexClosure() {

            @Override
            public void run(Status status, long index, byte[] reqCtx) {
              QuerySeriesTypeResponse response;
              if (status.isOk()) {
                try {
                  response = QuerySeriesTypeResponse.createSuccessResponse(groupId, mManager.getSeriesType(request.getPath()));
                  response.addResult(true);
                } catch (final PathErrorException e) {
                  response = QuerySeriesTypeResponse.createErrorResponse(groupId, e.getMessage());
                  response.addResult(false);
                }
              } else {
                response = QuerySeriesTypeResponse
                    .createErrorResponse(groupId, status.getErrorMsg());
                response.addResult(false);
              }
              asyncContext.sendResponse(response);
            }
          });
    }
  }

  @Override
  public String interest() {
    return QuerySeriesTypeRequest.class.getName();
  }
}
