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
package org.apache.iotdb.cluster.qp.executor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import com.alipay.sofa.jraft.util.Bits;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.cluster.callback.SingleTask;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.MetadataType;
import org.apache.iotdb.cluster.rpc.request.QueryMetadataRequest;
import org.apache.iotdb.cluster.rpc.response.QueryMetadataResponse;
import org.apache.iotdb.db.exception.PathErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle show all storage group logic
 */
public class QueryMetadataExecutor extends ClusterQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryMetadataExecutor.class);

  private final AtomicInteger requestId = new AtomicInteger(0);
  private final Server server = Server.getInstance();

  public QueryMetadataExecutor() {

  }

  public void init() {
    this.cliClientService = new BoltCliClientService();
    this.cliClientService.init(new CliOptions());
    SUB_TASK_NUM = 1;
  }

  public Set<String> processMetadataQuery(MetadataType type)
      throws InterruptedException {
    QueryMetadataRequest request = new QueryMetadataRequest(
        ClusterConfig.METADATA_GROUP_ID, type);
    SingleTask task = new SingleTask(false, request);
    return asyncHandleTaskLocally(task);
  }

  private Set<String> asyncHandleTaskLocally(SingleTask task) throws InterruptedException {
    readIndexForSG(task);

    task.await();
    return ((QueryMetadataResponse) task.getResponse()).getMetadataSet();
  }

  private void readIndexForSG(SingleTask task) {
    final byte[] reqContext = new byte[4];
    Bits.putInt(reqContext, 0, requestId.incrementAndGet());
    MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
    ((RaftService) metadataHolder.getService()).getNode()
        .readIndex(reqContext, new ReadIndexClosure() {

          @Override
          public void run(Status status, long index, byte[] reqCtx) {
            QueryMetadataResponse response = null;
            if (status.isOk()) {
              try {
                response = new QueryMetadataResponse(false, true,
                    metadataHolder.getFsm().getAllStorageGroups());
              } catch (final PathErrorException e) {
                response = new QueryMetadataResponse(false, false, null, e.toString());
              }
            } else {
              response = new QueryMetadataResponse(false, false, null, null);
            }
            task.run(response);
          }
        });
  }
}
