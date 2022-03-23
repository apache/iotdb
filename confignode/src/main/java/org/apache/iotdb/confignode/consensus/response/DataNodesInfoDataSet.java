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
package org.apache.iotdb.confignode.consensus.response;

import org.apache.iotdb.confignode.partition.DataNodeInfo;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.HashMap;
import java.util.Map;

public class DataNodesInfoDataSet implements DataSet {

  private final Map<Integer, DataNodeInfo> infoMap;

  public DataNodesInfoDataSet() {
    this.infoMap = new HashMap<>();
  }

  public void addDataNodeInfo(int dataNodeID, DataNodeInfo info) {
    this.infoMap.put(dataNodeID, info);
  }

  public Map<Integer, DataNodeInfo> getInfoMap() {
    return this.infoMap;
  }
}
