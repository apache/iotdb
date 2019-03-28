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
import java.io.IOException;
import org.apache.iotdb.cluster.callback.SingleTask;
import org.apache.iotdb.cluster.callback.Task;
import org.apache.iotdb.cluster.callback.Task.TaskState;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.NodeAsClient;
import org.apache.iotdb.cluster.rpc.impl.RaftNodeAsClient;
import org.apache.iotdb.cluster.rpc.request.ChangeMetadataRequest;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle distributed non-query logic
 */
public class NonQueryExecutor extends ClusterQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NonQueryExecutor.class);

  public NonQueryExecutor() {

  }

  public void init(){
    this.cliClientService = new BoltCliClientService();
    this.cliClientService.init(new CliOptions());
    SUB_TASK_NUM = 1;
  }

  public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
    try {
      switch (plan.getOperatorType()) {
        case DELETE:
          return delete((DeletePlan) plan);
        case UPDATE:
          return update((UpdatePlan) plan);
        case INSERT:
          return insert((InsertPlan) plan);
        case CREATE_ROLE:
        case DELETE_ROLE:
        case CREATE_USER:
        case REVOKE_USER_ROLE:
        case REVOKE_ROLE_PRIVILEGE:
        case REVOKE_USER_PRIVILEGE:
        case GRANT_ROLE_PRIVILEGE:
        case GRANT_USER_PRIVILEGE:
        case GRANT_USER_ROLE:
        case MODIFY_PASSWORD:
        case DELETE_USER:
        case LIST_ROLE:
        case LIST_USER:
        case LIST_ROLE_PRIVILEGE:
        case LIST_ROLE_USERS:
        case LIST_USER_PRIVILEGE:
        case LIST_USER_ROLES:
          return operateAuthor((AuthorPlan) plan);
        case LOADDATA:
          return loadData((LoadDataPlan) plan);
        case DELETE_TIMESERIES:
        case SET_STORAGE_GROUP:
        case METADATA:
          return operateMetadata((MetadataPlan) plan);
        case PROPERTY:
          return operateProperty((PropertyPlan) plan);
        default:
          throw new UnsupportedOperationException(
              String.format("operation %s does not support", plan.getOperatorType()));
      }
    } catch (RaftConnectionException e) {
      LOGGER.error("Raft connection occurs error.");
      throw new ProcessorException(e);
    } catch (InterruptedException | PathErrorException | IOException e) {
      throw new ProcessorException(e);
    }
  }

  //TODO
  private boolean update(UpdatePlan updatePlan) {
    return false;
  }

  //TODO
  private boolean delete(DeletePlan deletePlan) {
    return false;
  }

  /**
   * Handle insert plan
   */
  private boolean insert(InsertPlan insertPlan)
      throws ProcessorException, PathErrorException, InterruptedException, IOException, RaftConnectionException {
    String deviceId = insertPlan.getDeviceId();
    String storageGroup = getStroageGroupByDevice(deviceId);
    return handleRequest(storageGroup, insertPlan);
  }

  //TODO
  private boolean operateAuthor(AuthorPlan authorPlan) {
    return false;
  }

  //TODO
  private boolean loadData(LoadDataPlan loadDataPlan) {
    return false;
  }

  /**
   * Handle metadata plan
   */
  private boolean operateMetadata(MetadataPlan metadataPlan)
      throws RaftConnectionException, ProcessorException, IOException, PathErrorException, InterruptedException {
    MetadataOperator.NamespaceType namespaceType = metadataPlan.getNamespaceType();
    Path path = metadataPlan.getPath();
    switch (namespaceType) {
      case ADD_PATH:
        String deviceId = path.getDevice();
        String storageGroup = getStroageGroupByDevice(deviceId);
        return handleRequest(storageGroup, metadataPlan);
      case DELETE_PATH:
        //TODO
        return false;
      case SET_FILE_LEVEL:
        boolean fileLevelExist = mManager.checkStorageLevelOfMTree(path.getFullPath());
        if (fileLevelExist) {
          throw new ProcessorException(
              String.format("File level %s already exists.", path.getFullPath()));
        } else {
          ChangeMetadataRequest request = new ChangeMetadataRequest(
              ClusterConfig.METADATA_GROUP_ID,
              metadataPlan);
          PeerId leader = RaftUtils.getLeader(ClusterConfig.METADATA_GROUP_ID, cliClientService);

          SingleTask task = new SingleTask(false, request);
          return asyncHandleTask(task, leader, 0);
        }
      default:
        throw new ProcessorException("unknown namespace type:" + namespaceType);
    }
  }

  //TODO
  private boolean operateProperty(PropertyPlan propertyPlan) {
    return false;
  }

  /**
   * Handle request by storage group and physical plan
   */
  private boolean handleRequest(String storageGroup, PhysicalPlan plan)
      throws ProcessorException, IOException, RaftConnectionException, InterruptedException {
    /** Check if the plan can be executed locally. **/
    if (canHandle(storageGroup, cliClientService)) {
      return qpExecutor.processNonQuery(plan);
    } else {
      String groupId = getGroupIdBySG(storageGroup);
      ChangeMetadataRequest request = new ChangeMetadataRequest(groupId, plan);
      PeerId leader = RaftUtils.getLeader(groupId, cliClientService);

      SingleTask task = new SingleTask(false, request);
      return asyncHandleTask(task, leader, 0);
    }
  }

  /**
   * Async handle task by task and leader id.
   *
   * @param task request task
   * @param leader leader of the target raft group
   * @param taskRetryNum Number of task retries due to timeout and redirected.
   * @return request result
   */
  private boolean asyncHandleTask(Task task, PeerId leader, int taskRetryNum)
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
    return task.getResponse().isSuccess();
  }
}
