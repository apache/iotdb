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
package org.apache.iotdb.cluster.rpc.raft.processor.querymetric;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import org.apache.iotdb.cluster.rpc.raft.processor.BasicAsyncUserProcessor;
import org.apache.iotdb.cluster.rpc.raft.request.querymetric.QueryMetricRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querymetric.QueryMetricResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;

public class QueryMetricAsyncProcessor extends BasicAsyncUserProcessor<QueryMetricRequest> {

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      QueryMetricRequest request) {
    String groupId = request.getGroupID();

    QueryMetricResponse response = QueryMetricResponse.createSuccessResponse(groupId,
        RaftUtils.getReplicaMetric(request.getGroupID(), request.getMetric()));
    response.addResult(true);
    asyncContext.sendResponse(response);
  }

  @Override
  public String interest() {
    return QueryMetricRequest.class.getName();
  }
}
