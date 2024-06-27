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

package org.apache.iotdb.consensus.pipe.consensuspipe;

import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.PipeConsensusConfig;
import org.apache.iotdb.consensus.config.PipeConsensusConfig.ReplicateMode;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_CONSENSUS_GROUP_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_CONSENSUS_PIPE_NAME;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_REALTIME_FIRST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_CONSENSUS_GROUP_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_CONSENSUS_RECEIVER_DATANODE_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_KEY;

public class ConsensusPipeManager {
  private final PipeConsensusConfig.Pipe config;
  private final ReplicateMode replicateMode;
  private final ConsensusPipeDispatcher dispatcher;
  private final ConsensusPipeSelector selector;

  public ConsensusPipeManager(PipeConsensusConfig.Pipe config, ReplicateMode replicateMode) {
    this.config = config;
    this.replicateMode = replicateMode;
    this.dispatcher = config.getConsensusPipeDispatcher();
    this.selector = config.getConsensusPipeSelector();
  }

  public void createConsensusPipe(Peer senderPeer, Peer receiverPeer) throws Exception {
    ConsensusPipeName consensusPipeName = new ConsensusPipeName(senderPeer, receiverPeer);
    dispatcher.createPipe(
        consensusPipeName.toString(),
        ImmutableMap.<String, String>builder()
            .put(EXTRACTOR_KEY, config.getExtractorPluginName())
            .put(
                EXTRACTOR_CONSENSUS_GROUP_ID_KEY,
                consensusPipeName.getConsensusGroupId().toString())
            .put(
                EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY,
                String.valueOf(consensusPipeName.getSenderDataNodeId()))
            .put(
                EXTRACTOR_CONSENSUS_RECEIVER_DATANODE_ID_KEY,
                String.valueOf(consensusPipeName.getReceiverDataNodeId()))
            .put(EXTRACTOR_REALTIME_MODE_KEY, replicateMode.getValue())
            .build(),
        ImmutableMap.<String, String>builder()
            .put(PROCESSOR_KEY, config.getProcessorPluginName())
            .build(),
        ImmutableMap.<String, String>builder()
            .put(CONNECTOR_KEY, config.getConnectorPluginName())
            .put(
                CONNECTOR_CONSENSUS_GROUP_ID_KEY,
                String.valueOf(consensusPipeName.getConsensusGroupId().getId()))
            .put(CONNECTOR_CONSENSUS_PIPE_NAME, consensusPipeName.toString())
            .put(CONNECTOR_IOTDB_IP_KEY, receiverPeer.getEndpoint().ip)
            .put(CONNECTOR_IOTDB_PORT_KEY, String.valueOf(receiverPeer.getEndpoint().port))
            .put(CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, String.valueOf(1))
            .put(CONNECTOR_REALTIME_FIRST_KEY, String.valueOf(false))
            .build());
  }

  public void dropConsensusPipe(Peer senderPeer, Peer receiverPeer) throws Exception {
    ConsensusPipeName consensusPipeName = new ConsensusPipeName(senderPeer, receiverPeer);
    dispatcher.dropPipe(consensusPipeName);
  }

  public void updateConsensusPipe(ConsensusPipeName consensusPipeName, PipeStatus pipeStatus)
      throws Exception {
    if (PipeStatus.RUNNING.equals(pipeStatus)) {
      dispatcher.startPipe(consensusPipeName.toString());
    } else if (PipeStatus.STOPPED.equals(pipeStatus)) {
      dispatcher.stopPipe(consensusPipeName.toString());
    } else if (PipeStatus.DROPPED.equals(pipeStatus)) {
      dispatcher.dropPipe(consensusPipeName);
    } else {
      throw new IllegalArgumentException("Unsupported pipe status: " + pipeStatus);
    }
  }

  public Map<ConsensusPipeName, PipeStatus> getAllConsensusPipe() {
    return selector.getAllConsensusPipe();
  }
}
