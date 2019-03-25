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
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.iotdb.cluster.closure.SingleTaskClosure;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.bolt.NodeAsClient;
import org.apache.iotdb.cluster.rpc.bolt.request.NonQueryRequest;
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

public class NonQueryExecutor extends ClusterQPExecutor {

  private OverflowQPExecutor qpExecutor = new OverflowQPExecutor();
  private MManager mManager = MManager.getInstance();

  public boolean processNonQuery(PhysicalPlan plan)
      throws ProcessorException, IOException, RaftConnectionException, PathErrorException {
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
  }

  //TODO
  private boolean update(UpdatePlan updatePlan) {
    return false;
  }

  //TODO
  private boolean delete(DeletePlan deletePlan) {
    return false;
  }

  private boolean insert(InsertPlan insertPlan)
      throws ProcessorException, IOException, RaftConnectionException, PathErrorException {
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

  private boolean operateMetadata(MetadataPlan metadataPlan)
      throws RaftConnectionException, ProcessorException, IOException, PathErrorException {
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
        boolean fileLevelExist = false;
        try {
          fileLevelExist = mManager.checkFileLevel(path.getFullPath());
        } catch (PathErrorException e) {
        }
        if (fileLevelExist) {
          throw new ProcessorException(
              String.format("File level %s already exists.", path.getFullPath()));
        } else {
          return asyncHandleRequestByClient(getMetadataGroupID(), metadataPlan);
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
      throws ProcessorException, IOException, RaftConnectionException {
    /** Check if the plan can be executed locally. **/
    if (canHandle(storageGroup)) {
      return qpExecutor.processNonQuery(plan);
    } else {
      String groupId = getGroupIdBySG(storageGroup);
      return asyncHandleRequestByClient(groupId, plan);
    }
  }

  /**
   * Async handle request by group id and physical plan.
   */
  private boolean asyncHandleRequestByClient(String groupId, PhysicalPlan plan)
      throws IOException, RaftConnectionException {
    NonQueryRequest request = new NonQueryRequest(groupId, plan);
    PeerId leader = RaftUtils.getLeader(groupId);

    CountDownLatch countDownLatch = new CountDownLatch(1);
    SingleTaskClosure taskClosure = new SingleTaskClosure(countDownLatch, request, false);
    NodeAsClient client = new NodeAsClient();
    /** Call async method **/
    client.syncHandleRequest(request, leader, taskClosure);
    return taskClosure.getBasicResponse().isSuccess();
  }

}
