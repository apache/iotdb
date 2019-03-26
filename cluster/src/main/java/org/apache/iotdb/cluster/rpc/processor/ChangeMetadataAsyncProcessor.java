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
import com.alipay.sofa.jraft.entity.Task;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.rpc.request.ChangeMetadataRequest;
import org.apache.iotdb.cluster.rpc.response.NonQueryResponse;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeMetadataAsyncProcessor extends BasicAsyncUserProcessor<ChangeMetadataRequest> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeMetadataAsyncProcessor.class);
  private Server server;

  public ChangeMetadataAsyncProcessor(Server server) {
    this.server = server;
  }

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      ChangeMetadataRequest changeMetadataRequest) {
    String requestType = String.valueOf(changeMetadataRequest.getRequestType());
    if (requestType.equals(String.valueOf(OperatorType.SET_STORAGE_GROUP))) {
      MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
      /** Verify if it's the leader of metadata **/
      if (metadataHolder.getFsm().isLeader()) {
        try {
          final Task task = new Task();
          task.setDone(status -> {
            if (!status.isOk()) {
              asyncContext.sendResponse(new NonQueryResponse(false, false));
            }
            asyncContext.sendResponse(new NonQueryResponse(false, true));
          });
          task.setData(ByteBuffer
              .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
                  .serialize(changeMetadataRequest)));
          RaftService service = (RaftService) metadataHolder.getService();
          service.getNode().apply(task);
        } catch (final CodecException e) {
          asyncContext.sendResponse(new NonQueryResponse(false, false));
        }
      } else {
        NonQueryResponse response = new NonQueryResponse(true, false);
        asyncContext.sendResponse(response);
      }
    } else {
      //TODO deal with add path
      NonQueryResponse response = new NonQueryResponse(false, false);
      asyncContext.sendResponse(response);
    }
  }

  @Override
  public String interest() {
    return ChangeMetadataRequest.class.getName();
  }
}
