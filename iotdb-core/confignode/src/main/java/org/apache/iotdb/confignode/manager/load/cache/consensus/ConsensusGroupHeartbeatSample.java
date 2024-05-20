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

package org.apache.iotdb.confignode.manager.load.cache.consensus;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;

/** ConsensusGroupHeartbeatSample records the heartbeat sample of a consensus group. */
public class ConsensusGroupHeartbeatSample extends AbstractHeartbeatSample {

  private final int leaderId;

  public ConsensusGroupHeartbeatSample(long sampleLogicalTimestamp, int leaderId) {
    super(sampleLogicalTimestamp);
    this.leaderId = leaderId;
  }

  @TestOnly
  public ConsensusGroupHeartbeatSample(int leaderId) {
    super(System.nanoTime());
    this.leaderId = leaderId;
  }

  public int getLeaderId() {
    return leaderId;
  }
}
