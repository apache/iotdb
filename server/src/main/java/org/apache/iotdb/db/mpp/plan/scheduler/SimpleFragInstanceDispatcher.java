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

package org.apache.iotdb.db.mpp.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.WriteFragmentParallelPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.DeleteTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.DeleteTimeSeriesSchemaNode;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SimpleFragInstanceDispatcher implements IFragInstanceDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleFragInstanceDispatcher.class);
  private final ExecutorService executor;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;

  public SimpleFragInstanceDispatcher(
      ExecutorService executor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.executor = executor;
    this.internalServiceClientManager = internalServiceClientManager;
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(List<FragmentInstance> instances) {
    if (instances.size() == 1) {
      FragmentInstance fragmentInstance = instances.get(0);
      PlanNode root = fragmentInstance.getFragment().getRoot();
      if (root instanceof DeleteTimeSeriesNode) {
        return executor.submit(() -> dispatchDeleteTimeSeriesFragmentInstance(fragmentInstance));
      }
    }
    return executor.submit(() -> dispatchWriteFragmentInstance(instances));
  }

  private FragInstanceDispatchResult dispatchDeleteTimeSeriesFragmentInstance(
      FragmentInstance fragmentInstance) throws TException, IOException {
    DeleteTimeSeriesNode deleteTimeSeriesNode =
        (DeleteTimeSeriesNode) fragmentInstance.getFragment().getRoot();
    PlanFragment rootFragment = fragmentInstance.getFragment();
    List<PartialPath> deletedPaths = deleteTimeSeriesNode.getDeletedPaths();
    // TODO: Consider partcial deletion
    List<Boolean> deleteResults = new ArrayList<>(deletedPaths.size());
    Map<PartialPath, List<PlanNode>> dataRegionSplitMap =
        deleteTimeSeriesNode.getDataRegionSplitMap();
    Map<PartialPath, PlanNode> schemaRegionSpiltMap =
        deleteTimeSeriesNode.getSchemaRegionSpiltMap();
    List<FragmentInstance> dataRegionFragmentInstances = new ArrayList<>();
    List<FragmentInstance> schemaRegionFragmentInstances = new ArrayList<>();
    for (PartialPath deletedPath : deletedPaths) {
      // delete ts data
      dataRegionSplitMap.get(deletedPath).stream()
          .map(
              planNode ->
                  WriteFragmentParallelPlanner.wrapSplit(
                      rootFragment, null, (DeleteTimeSeriesSchemaNode) planNode, QueryType.WRITE))
          .forEach(dataRegionFragmentInstances::add);
      FragInstanceDispatchResult deleteDataResult =
          dispatchWriteFragmentInstance(dataRegionFragmentInstances);
      if (!deleteDataResult.isSuccessful()) {
        LOGGER.error("Delete TimeSeries data of {} failed.", dataRegionFragmentInstances);
        deleteResults.add(false);
        continue;
      }
      schemaRegionFragmentInstances.add(
          WriteFragmentParallelPlanner.wrapSplit(
              rootFragment,
              null,
              (DeleteTimeSeriesSchemaNode) schemaRegionSpiltMap.get(deletedPath),
              QueryType.WRITE));
      // delete ts schema
      FragInstanceDispatchResult deleteSchemaResult =
          dispatchWriteFragmentInstance(schemaRegionFragmentInstances);
      if (!deleteSchemaResult.isSuccessful()) {
        LOGGER.error("Delete TimeSeries schema of {} failed.", dataRegionFragmentInstances);
        deleteResults.add(false);
        continue;
      }
      deleteResults.add(true);
    }
    return new FragInstanceDispatchResult(deleteResults.stream().allMatch(Boolean::booleanValue));
  }

  private FragInstanceDispatchResult dispatchWriteFragmentInstance(List<FragmentInstance> instances)
      throws TException, IOException {
    TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp(false);
    for (FragmentInstance instance : instances) {
      TEndPoint endPoint = instance.getHostDataNode().getInternalEndPoint();
      // TODO: (jackie tien) change the port
      try (SyncDataNodeInternalServiceClient client =
          internalServiceClientManager.borrowClient(endPoint)) {
        // TODO: (xingtanzjr) consider how to handle the buffer here
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
        instance.serializeRequest(buffer);
        buffer.flip();
        TConsensusGroupId groupId = instance.getRegionReplicaSet().getRegionId();
        TSendFragmentInstanceReq req =
            new TSendFragmentInstanceReq(
                new TFragmentInstance(buffer), groupId, instance.getType().toString());
        LOGGER.info("send FragmentInstance[{}] to {}", instance.getId(), endPoint);
        resp = client.sendFragmentInstance(req);
      } catch (IOException | TException e) {
        LOGGER.error("can't connect to node {}", endPoint, e);
        throw e;
      }
      if (!resp.accepted) {
        break;
      }
    }
    return new FragInstanceDispatchResult(resp.accepted);
  }

  @Override
  public void abort() {}
}
