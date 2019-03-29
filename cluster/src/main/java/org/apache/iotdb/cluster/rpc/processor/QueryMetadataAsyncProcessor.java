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
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Bits;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.rpc.MetadataType;
import org.apache.iotdb.cluster.rpc.request.QueryMetadataRequest;
import org.apache.iotdb.cluster.rpc.response.QueryMetadataResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMetadataAsyncProcessor extends BasicAsyncUserProcessor<QueryMetadataRequest> {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryMetadataAsyncProcessor.class);
  private Server server;
  private final AtomicInteger requestId = new AtomicInteger(0);

  public QueryMetadataAsyncProcessor(Server server) {
    this.server = server;
  }

  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext,
      QueryMetadataRequest queryMetadataRequest) {
    String groupId = queryMetadataRequest.getGroupID();
    MetadataType metadataType = queryMetadataRequest.getMetadataType();
    if (metadataType == MetadataType.STORAGE_GROUP) {
      readIndexForSG(asyncContext);
    } else {
      //TODO deal with query time series
      QueryMetadataResponse response = new QueryMetadataResponse(false, false, null, null);
      asyncContext.sendResponse(response);
    }
  }

  private void readIndexForSG(AsyncContext asyncContext) {
    final byte[] reqContext = new byte[4];
    Bits.putInt(reqContext, 0, requestId.incrementAndGet());
    MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
    ((RaftService) metadataHolder.getService()).getNode()
        .readIndex(reqContext, new ReadIndexClosure() {

          @Override
          public void run(Status status, long index, byte[] reqCtx) {
            if (status.isOk()) {
              try {
                asyncContext.sendResponse(new QueryMetadataResponse(false, true,
                    metadataHolder.getFsm().getAllStorageGroups()));
              } catch (final PathErrorException e) {
                asyncContext
                    .sendResponse(new QueryMetadataResponse(false, false, null, e.toString()));
              }
            } else {
              asyncContext.sendResponse(new QueryMetadataResponse(false, false, null, null));
            }
          }
        });
  }

  @Override
  public String interest() {
    return QueryMetadataRequest.class.getName();
  }
}
