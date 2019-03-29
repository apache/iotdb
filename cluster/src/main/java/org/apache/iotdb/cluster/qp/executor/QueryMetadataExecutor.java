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

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.iotdb.cluster.callback.SingleTask;
import org.apache.iotdb.cluster.callback.Task;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.MetadataType;
import org.apache.iotdb.cluster.rpc.request.QueryMetadataRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.QueryMetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle show all storage group logic
 */
public class QueryMetadataExecutor extends ClusterQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryMetadataExecutor.class);

  public QueryMetadataExecutor() {

  }

  public void init(){
    this.cliClientService = new BoltCliClientService();
    this.cliClientService.init(new CliOptions());
    SUB_TASK_NUM = 1;
  }

  public Set<String> processMetadataQuery(MetadataType type)
      throws RaftConnectionException, InterruptedException {
    QueryMetadataRequest request = new QueryMetadataRequest(
        ClusterConfig.METADATA_GROUP_ID, type);
    SingleTask task = new SingleTask(false, request);
    return asyncHandleTaskLocally(task);
  }

  private Set<String> asyncHandleTaskLocally(SingleTask task)
      throws RaftConnectionException, InterruptedException {
    try {
      cliClientService.getRpcClient()
          .invokeWithCallback(localNode.toString(), task.getRequest(),
              new InvokeCallback() {

                @Override
                public void onResponse(Object result) {
                  BasicResponse response = (BasicResponse) result;
                  task.run(response);
                }

                @Override
                public void onException(Throwable e) {
                  LOGGER.error("Bolt rpc client occurs errors when handling Request", e);
                  task.setTaskState(TaskState.EXCEPTION);
                  task.run(null);

                }

                @Override
                public Executor getExecutor() {
                  return null;
                }
              }, CLUSTER_CONFIG.getTaskTimeoutMs());
    } catch (RemotingException | InterruptedException e) {
      throw new RaftConnectionException(e);
    }

    task.await();
    return ((QueryMetadataResponse) task.getResponse()).getMetadataSet();
  }
}
