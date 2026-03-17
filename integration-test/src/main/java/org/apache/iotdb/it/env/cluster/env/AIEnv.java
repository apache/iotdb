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

package org.apache.iotdb.it.env.cluster.env;

import org.apache.iotdb.itbase.runtime.NodeConnection;

import java.util.HashMap;
import java.util.Map;

public class AIEnv extends AbstractEnv {
  @Override
  public void initClusterEnvironment() {
    initClusterEnvironment(1, 1);
    checkActivationStatus(3);
  }

  @Override
  public void initClusterEnvironment(int configNodesNum, int dataNodesNum) {
    super.initEnvironment(configNodesNum, dataNodesNum, 600, true);
    checkActivationStatus(configNodesNum + dataNodesNum + 1);
  }

  @Override
  public void initClusterEnvironment(
      int configNodesNum, int dataNodesNum, int testWorkingRetryCount) {
    super.initEnvironment(configNodesNum, dataNodesNum, testWorkingRetryCount, true);
    checkActivationStatus(configNodesNum + dataNodesNum + 1);
  }

  @Override
  public NodeConnection getWriteConnection(
      Object o, String username3, String password3, String treeSqlDialect) {
    return null;
  }

  private void checkActivationStatus(int nodeCnt) {
    Map<Integer, String> activateMap = new HashMap<>();
    for (int i = 0; i < nodeCnt + 1; i++) {
      activateMap.put(i, "ACTIVATED");
    }
    checkActivationStatus(activate -> activateMap.values().stream().allMatch("ACTIVATED"::equals));
  }
}
