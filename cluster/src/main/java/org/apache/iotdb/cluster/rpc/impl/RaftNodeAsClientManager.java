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
package org.apache.iotdb.cluster.rpc.impl;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.util.concurrent.Executor;
import org.apache.iotdb.cluster.callback.QPTask;
import org.apache.iotdb.cluster.callback.QPTask.TaskState;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.rpc.NodeAsClient;
import org.apache.iotdb.cluster.rpc.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage resource of @NodeAsClient
 */
public class RaftNodeAsClientManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeAsClientManager.class);

  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  /**
   * Timeout limit for a task, the unit is milliseconds
   */
  private static final int TASK_TIMEOUT_MS = CLUSTER_CONFIG.getTaskTimeoutMs();

  /**
   * RaftNodeAsClient singleton
   */
  private static final RaftNodeAsClient client = new RaftNodeAsClient();

  /**
   * Max number of @NodeAsClient usage
   */
  private static final int MAX_CLIENT_NUM = CLUSTER_CONFIG.getMaxNumOfInnerRpcClient();

  /**
   *
   */
  private static final int MAX_QUEUE_CLIENT_NUM = CLUSTER_CONFIG.getMaxNumOfInnerRpcClient();

  public static final RaftNodeAsClientManager getInstance() {
    return RaftNodeAsClientManager.ClientManagerHolder.INSTANCE;
  }

  private static class ClientManagerHolder {

    private static final RaftNodeAsClientManager INSTANCE = new RaftNodeAsClientManager();

    private ClientManagerHolder() {

    }
  }

  /**
   * Implement NodeAsClient with Raft Service
   *
   * @see org.apache.iotdb.cluster.rpc.NodeAsClient
   */
  public static class RaftNodeAsClient implements NodeAsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeAsClient.class);

    private RaftNodeAsClient(){

    }

    @Override
    public void asyncHandleRequest(Object clientService, BasicRequest request, Object leader,
        QPTask qpTask)
        throws RaftConnectionException {
      BoltCliClientService boltClientService = (BoltCliClientService) clientService;
      PeerId raftLeader = (PeerId) leader;
      LOGGER.debug("Node as client to send request to leader:" + leader);
      try {
        boltClientService.getRpcClient()
            .invokeWithCallback(raftLeader.getEndpoint().toString(), request,
                new InvokeCallback() {

                  @Override
                  public void onResponse(Object result) {
                    BasicResponse response = (BasicResponse) result;
                    qpTask.run(response);
                  }

                  @Override
                  public void onException(Throwable e) {
                    LOGGER.error("Bolt rpc client occurs errors when handling Request", e);
                    qpTask.setTaskState(TaskState.EXCEPTION);
                    qpTask.run(null);

                  }

                  @Override
                  public Executor getExecutor() {
                    return null;
                  }
                }, TASK_TIMEOUT_MS);
      } catch (RemotingException | InterruptedException e) {
        LOGGER.error(e.toString());
        throw new RaftConnectionException(e);
      }
    }

    @Override
    public void syncHandleRequest(Object clientService, BasicRequest request, Object leader,
        QPTask QPTask)
        throws RaftConnectionException {
      BoltCliClientService boltClientService = (BoltCliClientService) clientService;
      PeerId raftLeader = (PeerId) leader;
      try {
        BasicResponse response = (BasicResponse) boltClientService.getRpcClient()
            .invokeSync(raftLeader.getEndpoint().toString(), request, TASK_TIMEOUT_MS);
        QPTask.run(response);
      } catch (RemotingException | InterruptedException e) {
        throw new RaftConnectionException(e);
      }
    }
  }


}
