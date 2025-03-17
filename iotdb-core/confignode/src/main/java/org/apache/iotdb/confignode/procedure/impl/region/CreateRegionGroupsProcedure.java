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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionCreateTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.CreateRegionGroupsState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class CreateRegionGroupsProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, CreateRegionGroupsState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateRegionGroupsProcedure.class);

  private TConsensusGroupType consensusGroupType;

  private CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
  private CreateRegionGroupsPlan persistPlan = new CreateRegionGroupsPlan();
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  /** key: TConsensusGroupId value: Failed RegionReplicas */
  private Map<TConsensusGroupId, TRegionReplicaSet> failedRegionReplicaSets = new HashMap<>();

  public CreateRegionGroupsProcedure() {
    super();
  }

  public CreateRegionGroupsProcedure(
      TConsensusGroupType consensusGroupType, CreateRegionGroupsPlan createRegionGroupsPlan) {
    this.consensusGroupType = consensusGroupType;
    this.createRegionGroupsPlan = createRegionGroupsPlan;
  }

  @TestOnly
  public CreateRegionGroupsProcedure(
      TConsensusGroupType consensusGroupType,
      CreateRegionGroupsPlan createRegionGroupsPlan,
      CreateRegionGroupsPlan persistPlan,
      Map<TConsensusGroupId, TRegionReplicaSet> failedRegionReplicaSets) {
    this.consensusGroupType = consensusGroupType;
    this.createRegionGroupsPlan = createRegionGroupsPlan;
    this.persistPlan = persistPlan;
    this.failedRegionReplicaSets = failedRegionReplicaSets;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateRegionGroupsState state) {
    switch (state) {
      case CREATE_REGION_GROUPS:
        failedRegionReplicaSets = env.doRegionCreation(consensusGroupType, createRegionGroupsPlan);
        setNextState(CreateRegionGroupsState.SHUNT_REGION_REPLICAS);
        break;
      case SHUNT_REGION_REPLICAS:
        persistPlan = new CreateRegionGroupsPlan();
        OfferRegionMaintainTasksPlan offerPlan = new OfferRegionMaintainTasksPlan();
        // Filter those RegionGroups that created successfully
        createRegionGroupsPlan
            .getRegionGroupMap()
            .forEach(
                (database, regionReplicaSets) ->
                    regionReplicaSets.forEach(
                        regionReplicaSet -> {
                          if (!failedRegionReplicaSets.containsKey(
                              regionReplicaSet.getRegionId())) {
                            // A RegionGroup was created successfully when
                            // all RegionReplicas were created successfully
                            persistPlan.addRegionGroup(database, regionReplicaSet);
                            LOGGER.info(
                                "[CreateRegionGroups] All replicas of RegionGroup: {} are created successfully!",
                                regionReplicaSet.getRegionId());
                          } else {
                            TRegionReplicaSet failedRegionReplicas =
                                failedRegionReplicaSets.get(regionReplicaSet.getRegionId());

                            boolean canProvideService =
                                canRegionGroupProvideService(
                                    regionReplicaSet.getDataNodeLocationsSize(),
                                    failedRegionReplicas.getDataNodeLocationsSize(),
                                    failedRegionReplicas.getRegionId());

                            if (canProvideService) {
                              // A RegionGroup can provide service as long as there are more than
                              // half of the RegionReplicas created successfully
                              persistPlan.addRegionGroup(database, regionReplicaSet);

                              // Build recreate tasks
                              failedRegionReplicas
                                  .getDataNodeLocations()
                                  .forEach(
                                      targetDataNode -> {
                                        RegionCreateTask createTask =
                                            new RegionCreateTask(
                                                targetDataNode, database, regionReplicaSet);
                                        offerPlan.appendRegionMaintainTask(createTask);
                                      });

                              LOGGER.info(
                                  "[CreateRegionGroups] Failed to create some replicas of RegionGroup: {}, but this RegionGroup can still be used.",
                                  regionReplicaSet.getRegionId());
                            } else {
                              // The redundant RegionReplicas should be deleted otherwise
                              regionReplicaSet
                                  .getDataNodeLocations()
                                  .forEach(
                                      targetDataNode -> {
                                        if (!failedRegionReplicas
                                            .getDataNodeLocations()
                                            .contains(targetDataNode)) {
                                          RegionDeleteTask deleteTask =
                                              new RegionDeleteTask(
                                                  targetDataNode, regionReplicaSet.getRegionId());
                                          offerPlan.appendRegionMaintainTask(deleteTask);
                                        }
                                      });

                              LOGGER.info(
                                  "[CreateRegionGroups] Failed to create most of replicas in RegionGroup: {}, The redundant replicas in this RegionGroup will be deleted.",
                                  regionReplicaSet.getRegionId());
                            }
                          }
                        }));

        env.persistRegionGroup(persistPlan);
        try {
          env.getConfigManager().getConsensusManager().write(offerPlan);
        } catch (ConsensusException e) {
          LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
        }
        setNextState(CreateRegionGroupsState.ACTIVATE_REGION_GROUPS);
        break;
      case ACTIVATE_REGION_GROUPS:
        long currentTime = System.nanoTime();
        // Build RegionGroupCache immediately to make these successfully built RegionGroup available
        Map<String, Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>>>
            activateRegionGroupMap = new TreeMap<>();
        createRegionGroupsPlan
            .getRegionGroupMap()
            .forEach(
                (database, regionReplicaSets) ->
                    regionReplicaSets.forEach(
                        regionReplicaSet -> {
                          TRegionReplicaSet failedRegionReplicas =
                              failedRegionReplicaSets.get(regionReplicaSet.getRegionId());

                          boolean canProvideService =
                              failedRegionReplicas == null
                                  || canRegionGroupProvideService(
                                      regionReplicaSet.getDataNodeLocationsSize(),
                                      failedRegionReplicas.getDataNodeLocationsSize(),
                                      failedRegionReplicas.getRegionId());

                          if (canProvideService) {
                            Set<Integer> failedDataNodeIds =
                                failedRegionReplicas == null
                                    ? new TreeSet<>()
                                    : failedRegionReplicas.getDataNodeLocations().stream()
                                        .map(TDataNodeLocation::getDataNodeId)
                                        .collect(Collectors.toSet());
                            Map<Integer, RegionHeartbeatSample> activateSampleMap = new TreeMap<>();
                            regionReplicaSet
                                .getDataNodeLocations()
                                .forEach(
                                    dataNodeLocation -> {
                                      int dataNodeId = dataNodeLocation.getDataNodeId();
                                      activateSampleMap.put(
                                          dataNodeId,
                                          new RegionHeartbeatSample(
                                              currentTime,
                                              failedDataNodeIds.contains(dataNodeId)
                                                  ? RegionStatus.Unknown
                                                  : RegionStatus.Running));
                                    });
                            activateRegionGroupMap
                                .computeIfAbsent(database, empty -> new TreeMap<>())
                                .put(regionReplicaSet.getRegionId(), activateSampleMap);
                          }
                        }));
        env.activateRegionGroup(activateRegionGroupMap);
        setNextState(CreateRegionGroupsState.CREATE_REGION_GROUPS_FINISH);
        break;
      case CREATE_REGION_GROUPS_FINISH:
        if (TConsensusGroupType.DataRegion.equals(consensusGroupType)) {
          // Re-balance all corresponding DataPartitionPolicyTable
          persistPlan
              .getRegionGroupMap()
              .keySet()
              .forEach(
                  database ->
                      env.getConfigManager()
                          .getLoadManager()
                          .reBalanceDataPartitionPolicy(database));
        }
        return Flow.NO_MORE_STATE;
    }

    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv,
      CreateRegionGroupsState createRegionGroupsState) {
    // Do nothing
  }

  @Override
  protected CreateRegionGroupsState getState(int stateId) {
    return CreateRegionGroupsState.values()[stateId];
  }

  @Override
  protected int getStateId(CreateRegionGroupsState createRegionGroupsState) {
    return createRegionGroupsState.ordinal();
  }

  @Override
  protected CreateRegionGroupsState getInitialState() {
    return CreateRegionGroupsState.CREATE_REGION_GROUPS;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    // Must serialize CREATE_REGION_GROUPS.getTypeCode() firstly
    stream.writeShort(ProcedureType.CREATE_REGION_GROUPS.getTypeCode());
    super.serialize(stream);
    stream.writeInt(consensusGroupType.getValue());
    createRegionGroupsPlan.serializeForProcedure(stream);
    stream.writeInt(failedRegionReplicaSets.size());
    failedRegionReplicaSets.forEach(
        (groupId, replica) -> {
          ThriftCommonsSerDeUtils.serializeTConsensusGroupId(groupId, stream);
          ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(replica, stream);
        });
    persistPlan.serializeForProcedure(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.consensusGroupType = TConsensusGroupType.findByValue(byteBuffer.getInt());
    try {
      createRegionGroupsPlan.deserializeForProcedure(byteBuffer);
      failedRegionReplicaSets.clear();
      int failedRegionsSize = byteBuffer.getInt();
      while (failedRegionsSize-- > 0) {
        TConsensusGroupId groupId =
            ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
        TRegionReplicaSet replica =
            ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(byteBuffer);
        failedRegionReplicaSets.put(groupId, replica);
      }
      if (byteBuffer.hasRemaining()) {
        persistPlan.deserializeForProcedure(byteBuffer);
      }
    } catch (Exception e) {
      LOGGER.error("Deserialize meets error in CreateRegionGroupsProcedure", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateRegionGroupsProcedure that = (CreateRegionGroupsProcedure) o;
    return consensusGroupType == that.consensusGroupType
        && createRegionGroupsPlan.equals(that.createRegionGroupsPlan)
        && persistPlan.equals(that.persistPlan)
        && failedRegionReplicaSets.equals(that.failedRegionReplicaSets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        consensusGroupType, createRegionGroupsPlan, persistPlan, failedRegionReplicaSets);
  }

  public boolean canRegionGroupProvideService(
      int regionGroupNodeNumber, int failedNodeNumber, TConsensusGroupId regionId) {
    boolean isStrongConsistency = CONF.isConsensusGroupStrongConsistency(regionId);
    int successNodeNumber = regionGroupNodeNumber - failedNodeNumber;
    if (isStrongConsistency) {
      return successNodeNumber > (regionGroupNodeNumber / 2);
    } else {
      return successNodeNumber >= 1;
    }
  }
}
