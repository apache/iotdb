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

package org.apache.iotdb.confignode.manager.load.cache;

/**
 * AbstractHeartbeatSample contains the sample logical timestamp. All kinds of heartbeat samples
 * should extend this class then expand its required fields.
 */
public abstract class AbstractHeartbeatSample {

  // Equals to the consensus group's logical timestamp for consensus heartbeat sample
  // Term for strong consistency consensus protocol such as Ratis
  // Log index for weak consistency consensus protocol such as IoT
  // 0 for no consensus such as Simple
  // Equals to the nano timestamp when the heartbeat request is sent for other heartbeat samples
  private final long sampleLogicalTimestamp;

  protected AbstractHeartbeatSample(long sampleLogicalTimestamp) {
    this.sampleLogicalTimestamp = sampleLogicalTimestamp;
  }

  public long getSampleLogicalTimestamp() {
    return sampleLogicalTimestamp;
  }
}
