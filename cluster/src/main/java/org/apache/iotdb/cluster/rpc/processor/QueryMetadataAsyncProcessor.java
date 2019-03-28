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
import java.util.Set;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.rpc.MetadataType;
import org.apache.iotdb.cluster.rpc.request.QueryMetadataRequest;
import org.apache.iotdb.cluster.rpc.response.QueryMetadataResponse;
import org.apache.iotdb.db.exception.PathErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMetadataAsyncProcessor extends BasicAsyncUserProcessor<QueryMetadataRequest> {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryMetadataAsyncProcessor.class);
  private Server server;

  public QueryMetadataAsyncProcessor(Server server) {
    this.server = server;
  }

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      QueryMetadataRequest queryMetadataRequest) {
    MetadataType metadataType = queryMetadataRequest.getMetadataType();
    if (metadataType == MetadataType.STORAGE_GROUP) {
      MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
      /** Verify if it's the leader of metadata **/
      if (metadataHolder.getFsm().isLeader()) {
        try {
          final Task task = new Task();
          Set<String> storageGroupSet = metadataHolder.getFsm().getAllStorageGroups();
          task.setDone(status -> {
            if (!status.isOk()) {
              asyncContext.sendResponse(new QueryMetadataResponse(false, false));
            }
            asyncContext.sendResponse(new QueryMetadataResponse(false, true, storageGroupSet));
          });
          task.setData(ByteBuffer
              .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
                  .serialize(queryMetadataRequest)));
          RaftService service = (RaftService) metadataHolder.getService();
          service.getNode().apply(task);
        } catch (final CodecException | PathErrorException e) {
          asyncContext.sendResponse(new QueryMetadataResponse(false, false));
        }
      } else {
        QueryMetadataResponse response = new QueryMetadataResponse(true, false);
        asyncContext.sendResponse(response);
      }
    } else {
      //TODO deal with query time series
      QueryMetadataResponse response = new QueryMetadataResponse(false, false);
      asyncContext.sendResponse(response);
    }
  }

  @Override
  public String interest() {
    return QueryMetadataRequest.class.getName();
  }
}
