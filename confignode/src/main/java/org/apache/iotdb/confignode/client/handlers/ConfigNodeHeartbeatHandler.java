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
package org.apache.iotdb.confignode.client.handlers;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.confignode.manager.load.heartbeat.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.HeartbeatPackage;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigNodeHeartbeatHandler implements AsyncMethodCallback<Long> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeHeartbeatHandler.class);

  // Update ConfigNodeHeartbeatCache when success
  private final TConfigNodeLocation configNodeLocation;
  private final ConfigNodeHeartbeatCache configNodeHeartbeatCache;

  public ConfigNodeHeartbeatHandler(
      TConfigNodeLocation configNodeLocation, ConfigNodeHeartbeatCache configNodeHeartbeatCache) {
    this.configNodeLocation = configNodeLocation;
    this.configNodeHeartbeatCache = configNodeHeartbeatCache;
  }

  @Override
  public void onComplete(Long timestamp) {
    configNodeHeartbeatCache.cacheHeartBeat(
        new HeartbeatPackage(timestamp, System.currentTimeMillis()));
  }

  @Override
  public void onError(Exception e) {
    LOGGER.warn(
        "Heartbeat error on ConfigNode: {id={}, internalEndPoint={}}",
        configNodeLocation.getConfigNodeId(),
        configNodeLocation.getInternalEndPoint(),
        e);
  }
}
