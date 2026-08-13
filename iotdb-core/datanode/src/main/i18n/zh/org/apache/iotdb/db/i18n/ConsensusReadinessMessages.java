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

package org.apache.iotdb.db.i18n;

public final class ConsensusReadinessMessages {

  public static final String
      LOG_CONSENSUS_IS_NOT_INITIALIZED_REJECTING_THE_REGION_TOPOLOGY_REQUEST_AFTER_WAITING_UP_TO_ARG_MS_7035CB1C =
      "Consensus 尚未初始化；等待最多 {} 毫秒后拒绝 region 拓扑请求";
  public static final String
      MESSAGE_CONSENSUS_IS_NOT_INITIALIZED_REGION_TOPOLOGY_REQUEST_REJECTED_AFTER_WAITING_UP_TO_ARG_MS_30E1CBCC =
      "Consensus 尚未初始化；等待最多 %d 毫秒后已拒绝 region 拓扑请求";

  private ConsensusReadinessMessages() {}
}
