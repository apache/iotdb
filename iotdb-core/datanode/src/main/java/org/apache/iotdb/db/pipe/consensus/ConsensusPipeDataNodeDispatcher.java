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

package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeDispatcher;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * No-op dispatcher. Consensus pipe lifecycle is fully managed by ConfigNode:
 *
 * <ul>
 *   <li>New DataRegion: ConfigNode creates pipes via CreatePipeProcedureV2
 *   <li>Region migration addPeer: ConfigNode creates pipes in AddRegionPeerProcedure
 *   <li>Region migration removePeer: ConfigNode drops pipes in RemoveRegionPeerProcedure
 * </ul>
 *
 * <p>The checkConsensusPipe guardian still detects inconsistencies (via logging), but actual
 * remediation is handled by ConfigNode's procedure retry or manual intervention.
 */
public class ConsensusPipeDataNodeDispatcher implements ConsensusPipeDispatcher {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusPipeDataNodeDispatcher.class);

  @Override
  public void createPipe(
      String pipeName,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes,
      boolean needManuallyStart)
      throws Exception {
    LOGGER.debug("No-op createPipe for {}, managed by ConfigNode", pipeName);
  }

  @Override
  public void startPipe(String pipeName) throws Exception {
    LOGGER.debug("No-op startPipe for {}, managed by ConfigNode", pipeName);
  }

  @Override
  public void stopPipe(String pipeName) throws Exception {
    LOGGER.debug("No-op stopPipe for {}, managed by ConfigNode", pipeName);
  }

  @Override
  public void dropPipe(ConsensusPipeName pipeName) throws Exception {
    LOGGER.debug("No-op dropPipe for {}, managed by ConfigNode", pipeName);
  }
}
