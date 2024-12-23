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

import org.apache.iotdb.it.env.cluster.EnvUtils;

import org.apache.tsfile.utils.Pair;

public class MultiClusterEnv extends AbstractEnv {

  public MultiClusterEnv(final long startTime, final int index, final String currentMethodName) {
    super(startTime);
    this.index = index;
    this.testMethodName = currentMethodName;
  }

  @Override
  public void initClusterEnvironment() {
    final Pair<Integer, Integer> nodeNum = EnvUtils.getNodeNum(index);
    super.initEnvironment(nodeNum.getLeft(), nodeNum.getRight());
  }

  @Override
  public void initClusterEnvironment(final int configNodesNum, final int dataNodesNum) {
    super.initEnvironment(configNodesNum, dataNodesNum);
  }

  @Override
  public void initClusterEnvironment(
      final int configNodesNum, final int dataNodesNum, final int testWorkingRetryCount) {
    super.initEnvironment(configNodesNum, dataNodesNum, testWorkingRetryCount);
  }
}
