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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.callback.BatchQPTask;
import org.apache.iotdb.cluster.callback.QPTask;
import org.apache.iotdb.cluster.callback.SingleQPTask;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.closure.ResponseClosure;
import org.apache.iotdb.cluster.rpc.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.request.DataGroupNonQueryRequest;
import org.apache.iotdb.cluster.rpc.request.MetaGroupNonQueryRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.DataGroupNonQueryResponse;
import org.apache.iotdb.cluster.rpc.service.TSServiceClusterImpl.BatchResult;
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

  /**
   * Execute batch statement by physical plans and update results.
   *
   * @param physicalPlans List of physical plan
   * @param batchResult batch result
   */
  public void processBatch(PhysicalPlan[] physicalPlans, BatchResult batchResult)
      throws InterruptedException {
    int[] result = batchResult.getResult();

    Map<String, List<PhysicalPlan>> physicalPlansMap = new HashMap<>();
    Map<String, List<Integer>> planIndexMap = new HashMap<>();
    for (int i = 0; i < result.length; i++) {
      if (result[i] != Statement.EXECUTE_FAILED) {
        PhysicalPlan plan = physicalPlans[i];
        String storageGroup;
        String groupId = null;
        try {
          switch (plan.getOperatorType()) {
            case DELETE:
              //TODO
              break;
            case UPDATE:
              Path path = ((UpdatePlan) plan).getPath();
              storageGroup = getStroageGroupByDevice(path.getDevice());
              groupId = getGroupIdBySG(storageGroup);
              break;
            case INSERT:
              storageGroup = getStroageGroupByDevice(((InsertPlan) plan).getDeviceId());
              groupId = getGroupIdBySG(storageGroup);
              break;
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
              groupId = CLUSTER_CONFIG.METADATA_GROUP_ID;
              break;
            case LOADDATA:
              //TODO
              break;
            case DELETE_TIMESERIES:
            case SET_STORAGE_GROUP:
            case METADATA:
              switch (((MetadataPlan) plan).getNamespaceType()) {
                case ADD_PATH:
                case DELETE_PATH:
                  String deviceId = ((MetadataPlan) plan).getPath().getDevice();
                  storageGroup = getStroageGroupByDevice(deviceId);
                  groupId = getGroupIdBySG(storageGroup);
                  break;
                case SET_FILE_LEVEL:
                  groupId = CLUSTER_CONFIG.METADATA_GROUP_ID;
                  break;
                default:
                  batchResult.setAllSuccessful(false);
                  batchResult.setBatchErrorMessage(
                      String.format("operation %s does not support", plan.getOperatorType()));
                  continue;
              }
              break;
            case PROPERTY:
              //TODO
            default:
              batchResult.setAllSuccessful(false);
              batchResult.setBatchErrorMessage(
                  String.format("operation %s does not support", plan.getOperatorType()));
              continue;
          }

          /** Divide into groups **/
          if (!physicalPlansMap.containsKey(groupId)) {
            physicalPlansMap.put(groupId, new ArrayList<>());
            planIndexMap.put(groupId, new ArrayList<>());
          }
          physicalPlansMap.get(groupId).add(plan);
          planIndexMap.get(groupId).add(i);
        } catch (Exception e) {
          result[i] = Statement.EXECUTE_FAILED;
          batchResult.setAllSuccessful(false);
          batchResult.setBatchErrorMessage(e.toString());
        }
      }
    }

    /** Construct request **/
    Map<String, SingleQPTask> subTaskMap = new HashMap<>();
    for (Entry<String, List<PhysicalPlan>> entry : physicalPlansMap.entrySet()) {
      String groupId = entry.getKey();
      SingleQPTask singleQPTask;
      BasicRequest request;
      try {
        request = new DataGroupNonQueryRequest(groupId, entry.getValue());
        singleQPTask = new SingleQPTask(false, request);
        subTaskMap.put(groupId, singleQPTask);
      } catch (IOException e) {
        batchResult.setAllSuccessful(false);
        batchResult.setBatchErrorMessage(e.toString());
        for (int index : planIndexMap.get(groupId)) {
          result[index] = Statement.EXECUTE_FAILED;
        }
      }
    }
    /** Execute multi task **/
    BatchQPTask task = new BatchQPTask(subTaskMap.size(), batchResult, subTaskMap, planIndexMap);
    task.execute(this);
    task.await();
    batchResult.setAllSuccessful(task.isAllSuccessful());
    batchResult.setBatchErrorMessage(task.getBatchErrorMessage());
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
    List<PhysicalPlan> plans = new ArrayList<>();
    plans.add(plan);
    DataGroupNonQueryRequest request = new DataGroupNonQueryRequest(groupId, plans);
    SingleQPTask qpTask = new SingleQPTask(false, request);
    /** Check if the plan can be executed locally. **/
    if (canHandleNonQueryBySG(storageGroup)) {
      return handleDataGroupRequestLocally(groupId, qpTask);
    } else {
      return asyncHandleTask(qpTask, leader);
    }
  }

  /**
   * Handle data group request locally.
   */
  public boolean handleDataGroupRequestLocally(String groupId, QPTask qpTask)
      throws InterruptedException {
    final Task task = new Task();
    BasicResponse response = new DataGroupNonQueryResponse(groupId, false, null, null);
    ResponseClosure closure = new ResponseClosure(response, status -> {
      response.addResult(status.isOk());
      if (!status.isOk()) {
        response.setErrorMsg(status.getErrorMsg());
      }
      qpTask.run(response);
    });
    task.setDone(closure);

    BasicRequest request = qpTask.getRequest();
    /** Apply qpTask to Raft Node **/
    try {
      task.setData(ByteBuffer
          .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
              .serialize(request)));
    } catch (final CodecException e) {
      return false;
    }
    DataPartitionRaftHolder dataRaftHolder = (DataPartitionRaftHolder) server
        .getDataPartitionHolderMap().get(groupId);
    RaftService service = (RaftService) dataRaftHolder.getService();
    service.getNode().apply(task);
    qpTask.await();
    return qpTask.getResponse().isSuccess();
  }

  /**
   * Async handle task by QPTask and leader id.
   *
   * @param task request QPTask
   * @param leader leader of the target raft group
   * @return request result
   */
  public boolean asyncHandleTask(QPTask task, PeerId leader)
      throws RaftConnectionException, InterruptedException {
    BasicResponse response = asyncHandleTaskGetRes(task, leader, 0);
    return response.isSuccess();
  }

  /**
   * Request need to broadcast in metadata group.
   *
   * @param plan Metadata Plan or Authoe Plan
   */
  public boolean redirectMetadataGroupLeader(PhysicalPlan plan)
      throws IOException, RaftConnectionException, InterruptedException {
    List<PhysicalPlan> plans = new ArrayList<>();
    plans.add(plan);
    MetaGroupNonQueryRequest request = new MetaGroupNonQueryRequest(
        CLUSTER_CONFIG.METADATA_GROUP_ID,
        plans);
    PeerId leader = RaftUtils.getTargetPeerID(CLUSTER_CONFIG.METADATA_GROUP_ID);

    SingleQPTask task = new SingleQPTask(false, request);
    return asyncHandleTask(task, leader);
  }
}
