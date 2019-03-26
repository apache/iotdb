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
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.rpc.request.NonQueryRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.NonQueryResponse;
import org.apache.iotdb.db.metadata.Metadata;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonQueryAsyncProcessor extends BasicAsyncUserProcessor<NonQueryRequest> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NonQueryAsyncProcessor.class);
  private PeerId localId;
  private Server server;

  public NonQueryAsyncProcessor(PeerId localId, Server server) {
    this.localId = localId;
    this.server = server;
  }

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      NonQueryRequest nonQueryRequest) {
    String requestType = String.valueOf(nonQueryRequest.getRequestType());
    if(requestType.equals(String.valueOf(OperatorType.SET_STORAGE_GROUP))){
      MetadataRaftHolder metadataHolder = (MetadataRaftHolder)server.getMetadataHolder();
      if(metadataHolder.getFsm().isLeader()){
        try {
          final Task task = new Task();
          task.setDone(status -> {
            if(!status.isOk()){
              asyncContext.sendResponse(new NonQueryResponse(false,false));
            }
            asyncContext.sendResponse(new NonQueryResponse(false, true));
          });
          task.setData(ByteBuffer
              .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(nonQueryRequest)));
//          metadataHoldercounterServer.getNode().apply(task);
        } catch (final CodecException e) {
          LOGGER.error("Fail to encode IncrementAndGetRequest", e);
          asyncContext.sendResponse(new NonQueryResponse(false, false));
        }
      }else{
        NonQueryResponse response = new NonQueryResponse(true, false);
        asyncContext.sendResponse(response);
      }
    }else{

    }
    if (!this.counterServer.getFsm().isLeader()) {
      asyncCtx.sendResponse(this.counterServer.redirect());
      return;
    }

    final ValueResponse response = new ValueResponse();
    final IncrementAndAddClosure closure = new IncrementAndAddClosure(counterServer, request, response,
        status -> {
          if (!status.isOk()) {
            response.setErrorMsg(status.getErrorMsg());
            response.setSuccess(false);
          }
          asyncCtx.sendResponse(response);
        });

    try {
      final Task task = new Task();
      task.setDone(closure);
      task.setData(ByteBuffer
          .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(request)));

      // apply task to raft group.
      counterServer.getNode().apply(task);
    } catch (final CodecException e) {
      LOG.error("Fail to encode IncrementAndGetRequest", e);
      response.setSuccess(false);
      response.setErrorMsg(e.getMessage());
      asyncCtx.sendResponse(response);
    }
  }

  @Override
  public String interest() {
    return NonQueryRequest.class.getName();
  }
}
