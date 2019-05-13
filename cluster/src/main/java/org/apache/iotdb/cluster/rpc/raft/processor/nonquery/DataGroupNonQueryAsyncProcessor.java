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
package org.apache.iotdb.cluster.rpc.raft.processor.nonquery;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.sofa.jraft.entity.PeerId;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.rpc.raft.processor.BasicAsyncUserProcessor;
import org.apache.iotdb.cluster.rpc.raft.request.nonquery.DataGroupNonQueryRequest;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.raft.response.nonquery.DataGroupNonQueryResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async handle those requests which need to be applied in data group.
 */
public class DataGroupNonQueryAsyncProcessor extends
    BasicAsyncUserProcessor<DataGroupNonQueryRequest> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(DataGroupNonQueryAsyncProcessor.class);

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      DataGroupNonQueryRequest request) {
    LOGGER.debug("Handle data non query request");

    /** Check if it's the leader **/
    String groupId = request.getGroupID();
    DataPartitionRaftHolder dataPartitionRaftHolder = RaftUtils.getDataPartitonRaftHolder(groupId);
    if (!dataPartitionRaftHolder.getFsm().isLeader()) {
      PeerId leader = RaftUtils.getLeaderPeerID(groupId);
      LOGGER.debug("Request need to redirect leader: {}, groupId : {} ", leader, groupId);

      DataGroupNonQueryResponse response = DataGroupNonQueryResponse
          .createRedirectedResponse(groupId, leader.toString());
      asyncContext.sendResponse(response);
    } else {
      LOGGER.debug("Apply task to raft node");

      /** Apply Task to Raft Node **/
      BasicResponse response = DataGroupNonQueryResponse.createEmptyResponse(groupId);
      RaftService service = (RaftService) dataPartitionRaftHolder.getService();
      RaftUtils.executeRaftTaskForRpcProcessor(service, asyncContext, request, response);
    }
  }

  @Override
  public String interest() {
    return DataGroupNonQueryRequest.class.getName();
  }
}
