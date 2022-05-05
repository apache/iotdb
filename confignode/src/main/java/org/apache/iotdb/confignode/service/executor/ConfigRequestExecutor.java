/*
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
package org.apache.iotdb.confignode.service.executor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.DataNodeInfo;
import org.apache.iotdb.confignode.persistence.PartitionInfo;
import org.apache.iotdb.consensus.common.DataSet;

public class ConfigRequestExecutor {

  private final DataNodeInfo dataNodeInfo;

  private final ClusterSchemaInfo clusterSchemaInfo;

  private final PartitionInfo partitionInfo;

  private final AuthorInfo authorInfo;

  public ConfigRequestExecutor() {
    this.dataNodeInfo = DataNodeInfo.getInstance();
    this.clusterSchemaInfo = ClusterSchemaInfo.getInstance();
    this.partitionInfo = PartitionInfo.getInstance();
    this.authorInfo = AuthorInfo.getInstance();
  }

  public DataSet executorQueryPlan(ConfigRequest req)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (req.getType()) {
      case GetDataNodeInfo:
        return dataNodeInfo.getDataNodeInfo((GetDataNodeInfoReq) req);
      case CountStorageGroup:
        return clusterSchemaInfo.countMatchedStorageGroups((CountStorageGroupReq) req);
      case GetStorageGroup:
        return clusterSchemaInfo.getMatchedStorageGroupSchemas((GetStorageGroupReq) req);
      case GetDataPartition:
      case GetOrCreateDataPartition:
        return partitionInfo.getDataPartition((GetDataPartitionReq) req);
      case GetSchemaPartition:
      case GetOrCreateSchemaPartition:
        return partitionInfo.getSchemaPartition((GetSchemaPartitionReq) req);
      case LIST_USER:
        return authorInfo.executeListUser();
      case LIST_ROLE:
        return authorInfo.executeListRole();
      case LIST_USER_PRIVILEGE:
        return authorInfo.executeListUserPrivileges((AuthorReq) req);
      case LIST_ROLE_PRIVILEGE:
        return authorInfo.executeListRolePrivileges((AuthorReq) req);
      case LIST_USER_ROLES:
        return authorInfo.executeListUserRoles((AuthorReq) req);
      case LIST_ROLE_USERS:
        return authorInfo.executeListRoleUsers((AuthorReq) req);
      default:
        throw new UnknownPhysicalPlanTypeException(req.getType());
    }
  }

  public TSStatus executorNonQueryPlan(ConfigRequest req)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (req.getType()) {
      case RegisterDataNode:
        return dataNodeInfo.registerDataNode((RegisterDataNodeReq) req);
      case SetStorageGroup:
        return clusterSchemaInfo.setStorageGroup((SetStorageGroupReq) req);
      case DeleteStorageGroup:
        return clusterSchemaInfo.deleteStorageGroup((DeleteStorageGroupReq) req);
      case SetTTL:
        return clusterSchemaInfo.setTTL((SetTTLReq) req);
      case SetSchemaReplicationFactor:
        return clusterSchemaInfo.setSchemaReplicationFactor((SetSchemaReplicationFactorReq) req);
      case SetDataReplicationFactor:
        return clusterSchemaInfo.setDataReplicationFactor((SetDataReplicationFactorReq) req);
      case SetTimePartitionInterval:
        return clusterSchemaInfo.setTimePartitionInterval((SetTimePartitionIntervalReq) req);
      case CreateRegions:
        return partitionInfo.createRegions((CreateRegionsReq) req);
      case DeleteRegions:
        return partitionInfo.deleteRegions((DeleteRegionsReq) req);
      case CreateSchemaPartition:
        return partitionInfo.createSchemaPartition((CreateSchemaPartitionReq) req);
      case CreateDataPartition:
        return partitionInfo.createDataPartition((CreateDataPartitionReq) req);
      case CREATE_USER:
      case CREATE_ROLE:
      case DROP_USER:
      case DROP_ROLE:
      case GRANT_ROLE:
      case GRANT_USER:
      case GRANT_ROLE_TO_USER:
      case REVOKE_USER:
      case REVOKE_ROLE:
      case REVOKE_ROLE_FROM_USER:
      case UPDATE_USER:
        return authorInfo.authorNonQuery((AuthorReq) req);
      default:
        throw new UnknownPhysicalPlanTypeException(req.getType());
    }
  }
}
