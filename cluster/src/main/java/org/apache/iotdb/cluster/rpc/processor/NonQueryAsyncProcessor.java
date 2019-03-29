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
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.rpc.request.NonQueryRequest;
import org.apache.iotdb.cluster.rpc.response.NonQueryResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.qp.logical.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async handle change metadata request.
 */
public class NonQueryAsyncProcessor extends BasicAsyncUserProcessor<NonQueryRequest> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NonQueryAsyncProcessor.class);
  private Server server;

  public NonQueryAsyncProcessor(Server server) {
    this.server = server;
  }

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      NonQueryRequest nonQueryRequest) {
    Operator.OperatorType requestType = nonQueryRequest.getRequestType();
    /** Check if it's the leader of metadata **/
    String groupId = nonQueryRequest.getGroupID();
    if (!this.server.getServerId().equals(RaftUtils.getTargetPeerID(groupId))) {
      PeerId leader = RaftUtils.getTargetPeerID(groupId);
      NonQueryResponse response = new NonQueryResponse(true, false, leader.toString(), null);
      asyncContext.sendResponse(response);
    }

    /** Apply Task to Raft Node **/
    final Task task = new Task();
    task.setDone((Status status) -> {
      asyncContext.sendResponse(
          new NonQueryResponse(false, status.isOk(), null, status.getErrorMsg()));
    });
    try {
      task.setData(ByteBuffer
          .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
              .serialize(nonQueryRequest)));
    } catch (final CodecException e) {
      asyncContext.sendResponse(new NonQueryResponse(false, false, null, e.toString()));
    }

    RaftService service;
    switch (requestType) {
      case SET_STORAGE_GROUP:
      case AUTHOR:
      case CREATE_USER:
      case CREATE_ROLE:
      case DELETE_ROLE:
      case DELETE_USER:
      case GRANT_USER_ROLE:
      case GRANT_USER_PRIVILEGE:
      case REVOKE_USER_PRIVILEGE:
      case REVOKE_USER_ROLE:
      case GRANT_ROLE_PRIVILEGE:
      case LIST_USER:
      case LIST_ROLE:
      case LIST_USER_PRIVILEGE:
      case LIST_ROLE_PRIVILEGE:
      case LIST_USER_ROLES:
      case LIST_ROLE_USERS:
      case MODIFY_PASSWORD:
        MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
        service = (RaftService) metadataHolder.getService();
        break;
      default:
        DataPartitionRaftHolder dataRaftHolder = (DataPartitionRaftHolder) server
            .getDataPartitionHolderMap().get(groupId);
        service = (RaftService) dataRaftHolder.getService();
    }
    service.getNode().apply(task);
  }

  @Override
  public String interest() {
    return NonQueryRequest.class.getName();
  }
}
