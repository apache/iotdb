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

import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeGuardian;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsensusPipeDataNodeRuntimeAgentGuardian implements ConsensusPipeGuardian {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusPipeDataNodeRuntimeAgentGuardian.class);
  private boolean registered = false;

  @Override
  public synchronized void start(String id, Runnable guardJob, long intervalInSeconds) {
    if (!registered) {
      LOGGER.info(
          "Registering periodical job {} with interval in seconds {}.", id, intervalInSeconds);

      this.registered = true;
      PipeDataNodeAgent.runtime().registerPeriodicalJob(id, guardJob, intervalInSeconds);
    }
  }

  @Override
  public synchronized void stop() {
    // Do nothing because PipePeriodicalJobExecutor currently has no deregister logic
  }
}
