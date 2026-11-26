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

package org.apache.iotdb.commons.utils.KillPoint;

/** Kill points for the ConfigNode-side region-maintain procedures (AddPeer / RemovePeer). */
public enum RegionMaintainKillPoints {
  /**
   * Fired from {@code RegionMaintainHandler.waitTaskFinish()} once the coordinator DataNode has
   * confirmed the task is still running (the first poll returned PROCESSING). Unlike the
   * AddRegionPeerState.DO_ADD_REGION_PEER kill point, which fires right after the task is submitted
   * and before waitTaskFinish() starts polling, this kill point guarantees the procedure worker is
   * actually blocked inside waitTaskFinish(). Tests use it to deterministically interrupt
   * waitTaskFinish() (e.g. by gracefully stopping the ConfigNode leader) so the PROCESSING branch
   * is exercised instead of racing the task to completion.
   */
  WAIT_TASK_FINISH_POLLING,
}
