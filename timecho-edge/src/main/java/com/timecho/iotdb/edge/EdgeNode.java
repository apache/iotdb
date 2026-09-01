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

package com.timecho.iotdb.edge;

import com.timecho.iotdb.DataNode;
import com.timecho.iotdb.service.ConfigNode;

/**
 * Entry point of the TimechoDB Edge distribution: starts the TimechoDB ConfigNode and DataNode
 * services inside ONE JVM process. The startup orchestration (bootstrap ordering, readiness
 * probing, failure propagation) is shared with {@link org.apache.iotdb.edge.EdgeNode}; this class
 * only plugs in the TimechoDB edition node bootstraps, mirroring how {@link
 * com.timecho.iotdb.service.ConfigNode} and {@link com.timecho.iotdb.DataNode} extend their apache
 * counterparts.
 *
 * <p>{@code sbin/start-edge.sh} and {@code sbin/windows/start-edge.bat} launch this class in both
 * editions (the edition gate handles feature differences at runtime), the same way the standard
 * launchers always start {@code com.timecho.iotdb.DataNode}.
 */
public final class EdgeNode {

  private EdgeNode() {}

  public static void main(String[] args) throws Exception {
    org.apache.iotdb.edge.EdgeNode.start(
        () -> ConfigNode.main(new String[] {"-s"}), () -> DataNode.main(new String[] {"-s"}));
  }
}
