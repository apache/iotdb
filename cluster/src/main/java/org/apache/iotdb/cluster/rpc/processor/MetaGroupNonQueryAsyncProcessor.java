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
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.rpc.closure.ResponseClosure;
import org.apache.iotdb.cluster.rpc.request.MetaGroupNonQueryRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.MetaGroupNonQueryResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async handle those requests which need to be applied in metadata group.
 */
public class MetaGroupNonQueryAsyncProcessor extends
    BasicAsyncUserProcessor<MetaGroupNonQueryRequest> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(MetaGroupNonQueryAsyncProcessor.class);
  private Server server;

  public MetaGroupNonQueryAsyncProcessor(Server server) {
    this.server = server;
  }

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      MetaGroupNonQueryRequest metaGroupNonQueryRequest) {
    LOGGER.info("Handle metadata non query query request.");

    /** Check if it's the leader **/
    String groupId = metaGroupNonQueryRequest.getGroupID();
    MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
    if (!metadataHolder.getFsm().isLeader()) {
      PeerId leader = RaftUtils.getLeaderPeerID(groupId);
      LOGGER.info("Request need to redirect leader: {}, groupId : {} ", leader, groupId);
      MetaGroupNonQueryResponse response = MetaGroupNonQueryResponse
          .createRedirectedInstance(groupId, leader.toString());
      asyncContext.sendResponse(response);
    } else {

      LOGGER.info("Apply task to metadata raft node");
      /** Apply Task to Raft Node **/
      final Task task = new Task();
      BasicResponse response = MetaGroupNonQueryResponse.createEmptyInstance(groupId);
      ResponseClosure closure = new ResponseClosure(response, status -> {
        response.addResult(status.isOk());
        if (!status.isOk()) {
          response.setErrorMsg(status.getErrorMsg());
        }
        asyncContext.sendResponse(response);
      });
      task.setDone(closure);
      try {
        task.setData(ByteBuffer
            .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
                .serialize(metaGroupNonQueryRequest)));
      } catch (final CodecException e) {
        response.addResult(false);
        response.setErrorMsg(e.toString());
        asyncContext.sendResponse(response);
      }

      RaftService service = (RaftService) metadataHolder.getService();
      service.getNode().apply(task);
    }
  }

  @Override
  public String interest() {
    return MetaGroupNonQueryRequest.class.getName();
  }
}
