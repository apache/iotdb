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

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.callback.QPTask;
import org.apache.iotdb.cluster.callback.SingleQPTask;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.request.DataNonQueryRequest;
import org.apache.iotdb.cluster.rpc.request.MetadataNonQueryRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.DataNonQueryResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
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

  public void init() {
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
      LOGGER.error(e.getMessage());
      throw new ProcessorException("Raft connection occurs error.", e);
    } catch (InterruptedException | PathErrorException | IOException e) {
      throw new ProcessorException(e);
    }
  }

  private boolean update(UpdatePlan updatePlan)
      throws PathErrorException, InterruptedException, RaftConnectionException, ProcessorException, IOException {
    Path path = updatePlan.getPath();
    String deviceId = path.getDevice();
    String storageGroup = getStroageGroupByDevice(deviceId);
    return handleDataGroupRequest(storageGroup, updatePlan);
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
    return handleDataGroupRequest(storageGroup, insertPlan);
  }

  /**
   * Handle author plan
   */
  private boolean operateAuthor(AuthorPlan authorPlan)
      throws IOException, RaftConnectionException, InterruptedException {
    return redirectMetadataGroupLeader(authorPlan);
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
      case DELETE_PATH:
        String deviceId = path.getDevice();
        String storageGroup = getStroageGroupByDevice(deviceId);
        return handleDataGroupRequest(storageGroup, metadataPlan);
      case SET_FILE_LEVEL:
        boolean fileLevelExist = mManager.checkStorageLevelOfMTree(path.getFullPath());
        if (fileLevelExist) {
          throw new ProcessorException(
              String.format("File level %s already exists.", path.getFullPath()));
        } else {
          LOGGER.info("Execute set storage group statement.");
          return redirectMetadataGroupLeader(metadataPlan);
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
   * Handle request to data group by storage group and physical plan
   */
  private boolean handleDataGroupRequest(String storageGroup, PhysicalPlan plan)
      throws IOException, RaftConnectionException, InterruptedException {
    String groupId = getGroupIdBySG(storageGroup);
    PeerId leader = RaftUtils.getTargetPeerID(groupId);
    DataNonQueryRequest request = new DataNonQueryRequest(groupId, plan);
    SingleQPTask qpTask = new SingleQPTask(false, request);
    /** Check if the plan can be executed locally. **/
    if (canHandleNonQuery(storageGroup)) {
      return handleDataGroupRequestLocally(groupId, qpTask, request);
    } else {
      return asyncHandleTask(qpTask, leader, 0);
    }
  }

  /**
   * Handle data group request locally.
   */
  private boolean handleDataGroupRequestLocally(String groupId, QPTask qpTask, BasicRequest request)
      throws InterruptedException {
    final byte[] reqContext = new byte[4];
    Task task = null;
    /** Apply qpTask to Raft Node **/
    try {
      task.setData(ByteBuffer
          .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
              .serialize(reqContext)));
    } catch (final CodecException e) {
      return false;
    }
    DataPartitionRaftHolder dataRaftHolder = (DataPartitionRaftHolder) server
        .getDataPartitionHolderMap().get(groupId);
    RaftService service = (RaftService) dataRaftHolder.getService();
    service.getNode().apply(task);
    qpTask.await();
    DataNonQueryResponse response = (DataNonQueryResponse) qpTask.getResponse();
    return response.isSuccess();
  }

  /**
   * Async handle task by QPTask and leader id.
   *
   * @param task request QPTask
   * @param leader leader of the target raft group
   * @param taskRetryNum Number of QPTask retries due to timeout and redirected.
   * @return request result
   */
  private boolean asyncHandleTask(QPTask task, PeerId leader, int taskRetryNum)
      throws RaftConnectionException, InterruptedException {
    BasicResponse response = asyncHandleTaskGetRes(task, leader, taskRetryNum);
    return response.isSuccess();
  }

  /**
   * Request need to broadcast in metadata group.
   *
   * @param plan Metadata Plan or Authoe Plan
   */
  public boolean redirectMetadataGroupLeader(PhysicalPlan plan)
      throws IOException, RaftConnectionException, InterruptedException {
    MetadataNonQueryRequest request = new MetadataNonQueryRequest(CLUSTER_CONFIG.METADATA_GROUP_ID,
        plan);
    PeerId leader = RaftUtils.getTargetPeerID(CLUSTER_CONFIG.METADATA_GROUP_ID);

    SingleQPTask task = new SingleQPTask(false, request);
    return asyncHandleTask(task, leader, 0);
  }
}
