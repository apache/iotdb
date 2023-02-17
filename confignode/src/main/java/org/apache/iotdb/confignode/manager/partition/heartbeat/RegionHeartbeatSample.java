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
package org.apache.iotdb.confignode.manager.partition.heartbeat;

import org.apache.iotdb.commons.cluster.RegionStatus;

public class RegionHeartbeatSample {

  // Unit: ms
  private final long sendTimestamp;
  private final long receiveTimestamp;
  private final RegionStatus status;

  // TODO: Add load sample

  public RegionHeartbeatSample(long sendTimestamp, long receiveTimestamp, RegionStatus status) {
    this.sendTimestamp = sendTimestamp;
    this.receiveTimestamp = receiveTimestamp;
    this.status = status;
  }

  public long getSendTimestamp() {
    return sendTimestamp;
  }

  public long getReceiveTimestamp() {
    return receiveTimestamp;
  }

  public RegionStatus getStatus() {
    return status;
  }
}
