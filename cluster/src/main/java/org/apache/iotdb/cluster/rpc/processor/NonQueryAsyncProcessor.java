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
import org.apache.iotdb.cluster.rpc.request.ChangeMatadataRequest;
import org.apache.iotdb.cluster.rpc.response.ChangeMetadataResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async handle change metadata request.
 */
public class ChangeMetadataAsyncProcessor extends BasicAsyncUserProcessor<ChangeMatadataRequest> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeMetadataAsyncProcessor.class);
  private Server server;

  public ChangeMetadataAsyncProcessor(Server server) {
    this.server = server;
  }

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      ChangeMatadataRequest changeMatadataRequest) {
    Operator.OperatorType requestType = changeMatadataRequest.getRequestType();
    /** Check if it's the leader of metadata **/
    String groupId = changeMatadataRequest.getGroupID();
    if (this.server.getServerId().equals(RaftUtils.getLeader(groupId))) {
      PeerId leader = RaftUtils.getLeader(groupId);
      ChangeMetadataResponse response = new ChangeMetadataResponse(true, false, leader.toString(), null);
      asyncContext.sendResponse(response);
    }
    /** Apply Task to Raft Node **/
    final Task task = new Task();
    task.setDone((Status status) -> {
      asyncContext.sendResponse(
          new ChangeMetadataResponse(false, status.isOk(), null, status.getErrorMsg()));
    });
    try {
      task.setData(ByteBuffer
          .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
              .serialize(changeMatadataRequest)));
    } catch (final CodecException e) {
      asyncContext.sendResponse(new ChangeMetadataResponse(false, false, null, e.toString()));
    }
    RaftService service;
    if (requestType == OperatorType.SET_STORAGE_GROUP) {
      MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
      service = (RaftService) metadataHolder.getService();
    } else {
      DataPartitionRaftHolder dataRaftHolder = (DataPartitionRaftHolder) server
          .getDataPartitionHolderMap().get(groupId);
      service = (RaftService) dataRaftHolder.getService();
    }
    service.getNode().apply(task);
  }

  @Override
  public String interest() {
    return ChangeMatadataRequest.class.getName();
  }
}
