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
package org.apache.iotdb.cluster.rpc.raft.processor.querydata;

import com.alipay.remoting.BizContext;
import org.apache.iotdb.cluster.query.manager.querynode.ClusterLocalQueryManager;
import org.apache.iotdb.cluster.rpc.raft.processor.BasicSyncUserProcessor;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;

public class QuerySeriesDataByTimestampSyncProcessor extends BasicSyncUserProcessor<QuerySeriesDataByTimestampRequest> {

  @Override
  public Object handleRequest(BizContext bizContext,
      QuerySeriesDataByTimestampRequest request) throws Exception {
    String groupId = request.getGroupID();
    QuerySeriesDataByTimestampResponse response = new QuerySeriesDataByTimestampResponse(groupId);
    ClusterLocalQueryManager.getInstance().readBatchDataByTimestamp(request, response);
    return response;
  }

  @Override
  public String interest() {
    return QuerySeriesDataByTimestampRequest.class.getName();
  }
}
