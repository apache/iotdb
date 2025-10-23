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

package org.apache.iotdb.db.queryengine.plan.planner.plan;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.enums.ReadConsistencyLevel;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.partition.QueryExecutor;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.planner.IFragmentParallelPlaner;
import org.apache.iotdb.db.queryengine.plan.planner.exceptions.ReplicaSetUnreachableException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.external.commons.collections4.CollectionUtils;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public abstract class AbstractFragmentParallelPlanner implements IFragmentParallelPlaner {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractFragmentParallelPlanner.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final ReadConsistencyLevel readConsistencyLevel;

  protected final MPPQueryContext queryContext;

  protected AbstractFragmentParallelPlanner(MPPQueryContext queryContext) {
    this.queryContext = queryContext;
    this.readConsistencyLevel = CONFIG.getReadConsistencyLevel();
  }

  protected void selectExecutorAndHost(
      PlanFragment fragment,
      FragmentInstance fragmentInstance,
      Supplier<TRegionReplicaSet> replicaSetProvider,
      UnaryOperator<TRegionReplicaSet> validator,
      Map<TDataNodeLocation, List<FragmentInstance>> dataNodeFIMap) {
    // Get the target region for origin PlanFragment, then its instance will be distributed one
    // of them.
    TRegionReplicaSet regionReplicaSet = replicaSetProvider.get();
    if (regionReplicaSet != null
        && !CollectionUtils.isEmpty(regionReplicaSet.getDataNodeLocations())) {
      regionReplicaSet = validator.apply(regionReplicaSet);
      if (regionReplicaSet.getDataNodeLocations().isEmpty()) {
        throw new ReplicaSetUnreachableException(replicaSetProvider.get());
      }
    }
    // Set ExecutorType and target host for the instance
    // We need to store all the replica host in case of the scenario that the instance need to be
    // redirected
    // to another host when scheduling
    if (regionReplicaSet == null || regionReplicaSet.getRegionId() == null) {
      TDataNodeLocation dataNodeLocation = fragment.getTargetLocation();
      if (dataNodeLocation != null) {
        // now only the case ShowQueries will enter here
        fragmentInstance.setExecutorAndHost(new QueryExecutor(dataNodeLocation));
      } else {
        // no data region && no dataNodeLocation, we need to execute this FI on local
        // now only the case AggregationQuery has schemaengine but no data region will enter here
        fragmentInstance.setExecutorAndHost(
            new QueryExecutor(DataNodeEndPoints.getLocalDataNodeLocation()));
      }
    } else {
      fragmentInstance.setExecutorAndHost(new StorageExecutor(regionReplicaSet));
      fragmentInstance.setHostDataNode(selectTargetDataNode(regionReplicaSet));
    }

    dataNodeFIMap.compute(
        fragmentInstance.getHostDataNode(),
        (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(fragmentInstance);
          return v;
        });
  }

  protected TDataNodeLocation selectTargetDataNode(TRegionReplicaSet regionReplicaSet) {
    if (regionReplicaSet == null
        || regionReplicaSet.getDataNodeLocations() == null
        || regionReplicaSet.getDataNodeLocations().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("regionReplicaSet is invalid: %s", regionReplicaSet));
    }
    boolean selectRandomDataNode = ReadConsistencyLevel.WEAK == this.readConsistencyLevel;

    // When planning fragment onto specific DataNode, the DataNode whose endPoint is in
    // black list won't be considered because it may have connection issue now.
    List<TDataNodeLocation> availableDataNodes =
        filterAvailableTDataNode(regionReplicaSet.getDataNodeLocations());
    if (availableDataNodes.isEmpty()) {
      String errorMsg =
          String.format(
              "All replicas for region[%s] are not available in these DataNodes[%s]",
              regionReplicaSet.getRegionId(), regionReplicaSet.getDataNodeLocations());
      throw new IoTDBRuntimeException(
          errorMsg, TSStatusCode.NO_AVAILABLE_REPLICA.getStatusCode(), true);
    }
    if (regionReplicaSet.getDataNodeLocationsSize() != availableDataNodes.size()) {
      LOGGER.info("available replicas: {}", availableDataNodes);
    }
    int targetIndex;
    if (!selectRandomDataNode || queryContext.getSession() == null) {
      targetIndex = 0;
    } else {
      targetIndex = (int) (queryContext.getSession().getSessionId() % availableDataNodes.size());
    }
    return availableDataNodes.get(targetIndex);
  }

  protected FragmentInstance findDownStreamInstance(
      Map<PlanNodeId, Pair<PlanFragmentId, PlanNode>> planNodeMap,
      Map<PlanFragmentId, FragmentInstance> instanceMap,
      PlanNodeId exchangeNodeId) {
    return instanceMap.get(planNodeMap.get(exchangeNodeId).left);
  }

  private List<TDataNodeLocation> filterAvailableTDataNode(
      List<TDataNodeLocation> originalDataNodeList) {
    List<TDataNodeLocation> result = new LinkedList<>();
    for (TDataNodeLocation dataNodeLocation : originalDataNodeList) {
      if (isAvailableDataNode(dataNodeLocation)) {
        result.add(dataNodeLocation);
      }
    }
    return result;
  }

  private boolean isAvailableDataNode(TDataNodeLocation dataNodeLocation) {
    for (TEndPoint endPoint : queryContext.getEndPointBlackList()) {
      if (endPoint.equals(dataNodeLocation.internalEndPoint)) {
        return false;
      }
    }
    return true;
  }
}
