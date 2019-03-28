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

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.util.Set;
import org.apache.iotdb.cluster.callback.SingleTask;
import org.apache.iotdb.cluster.callback.Task;
import org.apache.iotdb.cluster.callback.Task.TaskState;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.MetadataType;
import org.apache.iotdb.cluster.rpc.NodeAsClient;
import org.apache.iotdb.cluster.rpc.impl.RaftNodeAsClient;
import org.apache.iotdb.cluster.rpc.request.QueryMetadataRequest;
import org.apache.iotdb.cluster.rpc.response.QueryMetadataResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;

/**
 * Handle show all storage group logic
 */
public class QueryMetadataExecutor extends ClusterQPExecutor {

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
    PeerId leader = RaftUtils.getLeader(ClusterConfig.METADATA_GROUP_ID, cliClientService);

    SingleTask task = new SingleTask(false, request);
    return asyncHandleTask(task, leader, 0);
  }

  /**
   * Async handle task by task and leader id.
   *
   * @param task request task
   * @param leader leader of the target raft group
   * @param taskRetryNum Number of task retries due to timeout and redirected.
   * @return request result
   */
  private Set<String> asyncHandleTask(Task task, PeerId leader, int taskRetryNum)
      throws RaftConnectionException, InterruptedException {
    if (taskRetryNum >= TASK_MAX_RETRY) {
      throw new RaftConnectionException(String.format("Task retries reach the upper bound %s",
          TASK_MAX_RETRY));
    }
    NodeAsClient client = new RaftNodeAsClient();
    /** Call async method **/
    client.asyncHandleRequest(cliClientService, task.getRequest(), leader, task);
    task.await();
    if (task.getTaskState() != TaskState.FINISH) {
      task.setTaskNum(SUB_TASK_NUM);
      return asyncHandleTask(task, leader, taskRetryNum + 1);
    }
    return ((QueryMetadataResponse) task.getResponse()).getMetadataSet();
  }
}
