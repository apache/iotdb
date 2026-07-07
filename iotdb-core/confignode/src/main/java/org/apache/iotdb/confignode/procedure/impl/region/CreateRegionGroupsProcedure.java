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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionCreateTask;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.CreateRegionGroupsState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
      final TConsensusGroupType consensusGroupType,
      final CreateRegionGroupsPlan createRegionGroupsPlan) {
    this.consensusGroupType = consensusGroupType;
    this.createRegionGroupsPlan = createRegionGroupsPlan;
  }

  @TestOnly
  public CreateRegionGroupsProcedure(
      final TConsensusGroupType consensusGroupType,
      final CreateRegionGroupsPlan createRegionGroupsPlan,
      final CreateRegionGroupsPlan persistPlan,
      final Map<TConsensusGroupId, TRegionReplicaSet> failedRegionReplicaSets) {
    this.consensusGroupType = consensusGroupType;
    this.createRegionGroupsPlan = createRegionGroupsPlan;
    this.persistPlan = persistPlan;
    this.failedRegionReplicaSets = failedRegionReplicaSets;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final CreateRegionGroupsState state) {
    switch (state) {
      case CREATE_REGION_GROUPS:
        failedRegionReplicaSets = env.doRegionCreation(consensusGroupType, createRegionGroupsPlan);
        setNextState(CreateRegionGroupsState.SHUNT_REGION_REPLICAS);
        break;
      case SHUNT_REGION_REPLICAS:
        persistPlan = new CreateRegionGroupsPlan();
        final OfferRegionMaintainTasksPlan offerPlan = new OfferRegionMaintainTasksPlan();
        // RegionGroups that failed to reach a serving quorum have their redundant (already-created)
        // replicas removed via an independent root RemoveRegionGroupProcedure. Submitting them as
        // root procedures (instead of children) keeps this procedure from waiting for or being
        // failed by the cleanup: each one retries forever until those replicas are deleted, while
        // this procedure proceeds to activate the region groups that did form a quorum.
        final List<RemoveRegionGroupProcedure> removeRegionGroupProcedures = new ArrayList<>();
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
                                ProcedureMessages
                                    .CREATEREGIONGROUPS_ALL_REPLICAS_OF_REGIONGROUP_ARE_CREATED_SUCCESSFULLY,
                                regionReplicaSet.getRegionId());
                          } else {
                            final TRegionReplicaSet failedRegionReplicas =
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
                                  ProcedureMessages
                                      .CREATEREGIONGROUPS_FAILED_TO_CREATE_SOME_REPLICAS_OF_REGIONGROUP_BUT_THIS,
                                  regionReplicaSet.getRegionId());
                            } else {
                              // The redundant RegionReplicas (the ones that did get created) should
                              // be deleted otherwise
                              final TRegionReplicaSet redundantReplicas =
                                  new TRegionReplicaSet()
                                      .setRegionId(regionReplicaSet.getRegionId());
                              regionReplicaSet
                                  .getDataNodeLocations()
                                  .forEach(
                                      targetDataNode -> {
                                        if (!failedRegionReplicas
                                            .getDataNodeLocations()
                                            .contains(targetDataNode)) {
                                          redundantReplicas.addToDataNodeLocations(targetDataNode);
                                        }
                                      });
                              if (redundantReplicas.getDataNodeLocationsSize() > 0) {
                                removeRegionGroupProcedures.add(
                                    new RemoveRegionGroupProcedure(redundantReplicas));
                              }

                              LOGGER.info(
                                  ProcedureMessages
                                      .CREATEREGIONGROUPS_FAILED_TO_CREATE_MOST_OF_REPLICAS_IN_REGIONGROUP_THE,
                                  regionReplicaSet.getRegionId());
                            }
                          }
                        }));

        final TSStatus persistStatus = env.persistRegionGroup(persistPlan);
        if (persistStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          setFailure(new ProcedureException(new IoTDBException(persistStatus)));
          return Flow.NO_MORE_STATE;
        }
        try {
          env.getConfigManager().getConsensusManager().write(offerPlan);
        } catch (final ConsensusException e) {
          LOGGER.warn(
              ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
        }
        // Submit the redundant-replica cleanups as independent root procedures. This is
        // intentionally NOT guarded by isStateDeserialized(): the executor persists a procedure at
        // a state BEFORE that state's body has run (it advances the state on the previous cycle,
        // then may stop at the inter-state boundary on a leader switch — see
        // ProcedureExecutor#executeProcedure), so a recovery that lands on SHUNT_REGION_REPLICAS
        // means the submissions have NOT happened yet. Skipping them would leave the
        // already-created
        // replicas of sub-quorum region groups on disk with no cleanup and no partition-table
        // record
        // (the else branch above never persisted them). Re-submitting on recovery is safe instead:
        // the cleanups are recomputed from the serialized failedRegionReplicaSets, each gets a
        // fresh
        // procId and performs an idempotent delete, so a duplicate is harmless whereas a skip
        // leaks.
        removeRegionGroupProcedures.forEach(
            removeRegionGroupProcedure ->
                env.getConfigManager()
                    .getProcedureManager()
                    .getExecutor()
                    .submitProcedure(removeRegionGroupProcedure));
        setNextState(CreateRegionGroupsState.REBALANCE_DATA_PARTITION_POLICY);
        break;
      case REBALANCE_DATA_PARTITION_POLICY:
        if (TConsensusGroupType.DataRegion.equals(consensusGroupType)) {
          // Re-balance all corresponding DataPartitionPolicyTable before the newly created
          // RegionGroups become available for serving partitions.
          persistPlan
              .getRegionGroupMap()
              .keySet()
              .forEach(
                  database ->
                      env.getConfigManager()
                          .getLoadManager()
                          .reBalanceDataPartitionPolicy(database));
        }
        setNextState(CreateRegionGroupsState.ACTIVATE_REGION_GROUPS);
        break;
      case ACTIVATE_REGION_GROUPS:
        final long currentTime = System.nanoTime();
        // Build RegionGroupCache immediately to make these successfully built RegionGroup available
        final Map<String, Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>>>
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
                            final Set<Integer> failedDataNodeIds =
                                failedRegionReplicas == null
                                    ? new TreeSet<>()
                                    : failedRegionReplicas.getDataNodeLocations().stream()
                                        .map(TDataNodeLocation::getDataNodeId)
                                        .collect(Collectors.toSet());
                            final Map<Integer, RegionHeartbeatSample> activateSampleMap =
                                new TreeMap<>();
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
        setNextState(CreateRegionGroupsState.CREATE_INITIAL_CONSENSUS_PIPES);
        break;
      case CREATE_INITIAL_CONSENSUS_PIPES:
        if (TConsensusGroupType.DataRegion.equals(consensusGroupType)) {
          env.getRegionMaintainHandler().createInitialConsensusPipes(persistPlan);
        }
        setNextState(CreateRegionGroupsState.CREATE_REGION_GROUPS_FINISH);
        break;
      case CREATE_REGION_GROUPS_FINISH:
        return Flow.NO_MORE_STATE;
    }

    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv,
      final CreateRegionGroupsState createRegionGroupsState) {
    // Do nothing
  }

  @Override
  protected CreateRegionGroupsState getState(final int stateId) {
    return CreateRegionGroupsState.values()[stateId];
  }

  @Override
  protected int getStateId(final CreateRegionGroupsState createRegionGroupsState) {
    return createRegionGroupsState.ordinal();
  }

  @Override
  protected CreateRegionGroupsState getInitialState() {
    return CreateRegionGroupsState.CREATE_REGION_GROUPS;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
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
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.consensusGroupType = TConsensusGroupType.findByValue(byteBuffer.getInt());
    try {
      createRegionGroupsPlan.deserializeForProcedure(byteBuffer);
      failedRegionReplicaSets.clear();
      int failedRegionsSize = byteBuffer.getInt();
      while (failedRegionsSize-- > 0) {
        final TConsensusGroupId groupId =
            ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
        final TRegionReplicaSet replica =
            ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(byteBuffer);
        failedRegionReplicaSets.put(groupId, replica);
      }
      if (byteBuffer.hasRemaining()) {
        persistPlan.deserializeForProcedure(byteBuffer);
      }
    } catch (final Exception e) {
      LOGGER.error(ProcedureMessages.DESERIALIZE_MEETS_ERROR_IN_CREATEREGIONGROUPSPROCEDURE, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateRegionGroupsProcedure that = (CreateRegionGroupsProcedure) o;
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
