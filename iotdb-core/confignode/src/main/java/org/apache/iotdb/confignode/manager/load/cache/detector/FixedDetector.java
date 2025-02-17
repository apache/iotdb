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
package org.apache.iotdb.confignode.manager.load.cache.detector;

import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.IFailureDetector;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;

import org.apache.tsfile.utils.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * FixedDetector will decide a node unknown iff. Time elapsed from last heartbeat exceeds the
 * heartbeatTimeoutNs.
 */
public class FixedDetector implements IFailureDetector {
  private final long heartbeatTimeoutNs;

  public FixedDetector(long heartbeatTimeoutNs) {
    this.heartbeatTimeoutNs = heartbeatTimeoutNs;
  }

  @Override
  public boolean isAvailable(List<AbstractHeartbeatSample> history) {
    final AbstractHeartbeatSample lastSample =
        history.isEmpty() ? null : history.get(history.size() - 1);
    if (lastSample != null) {
      Preconditions.checkArgument(lastSample instanceof NodeHeartbeatSample);
    }
    final long lastSendTime =
        Optional.ofNullable(lastSample)
            .map(AbstractHeartbeatSample::getSampleLogicalTimestamp)
            .orElse(0L);
    final long currentNanoTime = System.nanoTime();
    return currentNanoTime - lastSendTime <= heartbeatTimeoutNs;
  }
}
