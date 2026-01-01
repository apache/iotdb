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

package org.apache.iotdb.consensus.air.airreplication;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.AirReplicationConfig;
import org.apache.iotdb.consensus.config.AirReplicationConfig.ReplicateMode;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.external.commons.lang3.tuple.ImmutableTriple;
import org.apache.tsfile.external.commons.lang3.tuple.Triple;

import java.util.Map;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_CONSENSUS_GROUP_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_CONSENSUS_PIPE_NAME;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_CONSENSUS_GROUP_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_CONSENSUS_RECEIVER_DATANODE_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY;

public class AirReplicationManager {
  // Extract data.insert and data.delete to support deletion.
  private static final String REPLICATION_EXTRACTOR_INCLUSION_VALUE = "data";
  private final AirReplicationConfig.Air config;
  private final ReplicateMode replicateMode;
  private final AirReplicationDispatcher dispatcher;
  private final AirReplicationSelector selector;

  public AirReplicationManager(AirReplicationConfig.Air config, ReplicateMode replicateMode) {
    this.config = config;
    this.replicateMode = replicateMode;
    this.dispatcher = config.getAirReplicationDispatcher();
    this.selector = config.getAirReplicationSelector();
  }

  /** This method is used except region migration. */
  public void createAirReplication(Peer senderPeer, Peer receiverPeer) throws Exception {
    AirReplicationName airReplicationName = new AirReplicationName(senderPeer, receiverPeer);
    // The third parameter is only used when region migration. Since this method is not called by
    // region migration, just pass senderPeer in to get the correct result.
    Triple<ImmutableMap<String, String>, ImmutableMap<String, String>, ImmutableMap<String, String>>
        params = buildAirParams(senderPeer, receiverPeer);
    dispatcher.createAir(
        airReplicationName.toString(),
        params.getLeft(),
        params.getMiddle(),
        params.getRight(),
        false);
  }

  /** This method is used when executing region migration */
  public void createAirReplication(Peer senderPeer, Peer receiverPeer, boolean needManuallyStart)
      throws Exception {
    AirReplicationName airReplicationName = new AirReplicationName(senderPeer, receiverPeer);
    Triple<ImmutableMap<String, String>, ImmutableMap<String, String>, ImmutableMap<String, String>>
        params = buildAirParams(senderPeer, receiverPeer);
    dispatcher.createAir(
        airReplicationName.toString(),
        params.getLeft(),
        params.getMiddle(),
        params.getRight(),
        needManuallyStart);
  }

  public Triple<
          ImmutableMap<String, String>, ImmutableMap<String, String>, ImmutableMap<String, String>>
      buildAirParams(final Peer senderPeer, final Peer receiverPeer) {
    final AirReplicationName airReplicationName = new AirReplicationName(senderPeer, receiverPeer);
    return new ImmutableTriple<>(
        ImmutableMap.<String, String>builder()
            .put(EXTRACTOR_KEY, config.getExtractorPluginName())
            .put(EXTRACTOR_INCLUSION_KEY, REPLICATION_EXTRACTOR_INCLUSION_VALUE)
            .put(
                EXTRACTOR_CONSENSUS_GROUP_ID_KEY,
                airReplicationName.getConsensusGroupId().toString())
            .put(
                EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY,
                String.valueOf(airReplicationName.getSenderDataNodeId()))
            .put(
                EXTRACTOR_CONSENSUS_RECEIVER_DATANODE_ID_KEY,
                String.valueOf(airReplicationName.getReceiverDataNodeId()))
            .put(EXTRACTOR_REALTIME_MODE_KEY, replicateMode.getValue())
            .put(EXTRACTOR_CAPTURE_TABLE_KEY, String.valueOf(true))
            .put(EXTRACTOR_CAPTURE_TREE_KEY, String.valueOf(true))
            .put(
                EXTRACTOR_IOTDB_USER_KEY,
                CommonDescriptor.getInstance().getConfig().getDefaultAdminName())
            .build(),
        ImmutableMap.<String, String>builder()
            .put(PROCESSOR_KEY, config.getProcessorPluginName())
            .build(),
        ImmutableMap.<String, String>builder()
            .put(CONNECTOR_KEY, config.getConnectorPluginName())
            .put(
                CONNECTOR_CONSENSUS_GROUP_ID_KEY,
                String.valueOf(airReplicationName.getConsensusGroupId().getId()))
            .put(CONNECTOR_CONSENSUS_PIPE_NAME, airReplicationName.toString())
            .put(CONNECTOR_IOTDB_IP_KEY, receiverPeer.getEndpoint().ip)
            .put(CONNECTOR_IOTDB_PORT_KEY, String.valueOf(receiverPeer.getEndpoint().port))
            .put(CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, String.valueOf(1))
            .put(CONNECTOR_REALTIME_FIRST_KEY, String.valueOf(false))
            .build());
  }

  public void dropAirReplication(Peer senderPeer, Peer receiverPeer) throws Exception {
    AirReplicationName airReplicationName = new AirReplicationName(senderPeer, receiverPeer);
    dispatcher.dropAir(airReplicationName);
  }

  public void updateAirReplication(AirReplicationName airReplicationName, PipeStatus pipeStatus)
      throws Exception {
    if (PipeStatus.RUNNING.equals(pipeStatus)) {
      dispatcher.startAir(airReplicationName.toString());
    } else if (PipeStatus.STOPPED.equals(pipeStatus)) {
      dispatcher.stopAir(airReplicationName.toString());
    } else if (PipeStatus.DROPPED.equals(pipeStatus)) {
      dispatcher.dropAir(airReplicationName);
    } else {
      throw new IllegalArgumentException("Unsupported air status: " + pipeStatus);
    }
  }

  public Map<AirReplicationName, PipeStatus> getAllAirReplication() {
    return selector.getAllAirReplication();
  }
}
