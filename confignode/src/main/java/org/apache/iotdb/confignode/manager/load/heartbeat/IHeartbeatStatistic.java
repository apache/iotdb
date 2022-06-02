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
package org.apache.iotdb.confignode.manager.load.heartbeat;

/** All the interfaces that provided by HeartbeatCache */
public interface IHeartbeatStatistic {

  /**
   * Cache the newest HeartbeatData of the specific DataNode
   *
   * @param dataNodeId The specific DataNodeId
   * @param newHeartbeat The newest HeartbeatData
   */
  void cacheHeartBeat(int dataNodeId, HeartbeatPackage newHeartbeat);

  // TODO: Interfaces for statistics

  /** Only use this interface when current ConfigNode is not the leader */
  void discardAllCache();
}
