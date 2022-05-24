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

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.ArrayList;
import java.util.List;

public class RegionReplicaSetInfo {
  private TRegionReplicaSet regionReplicaSet;
  private List<String> ownedStorageGroups;

  public RegionReplicaSetInfo(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
    this.ownedStorageGroups = new ArrayList<>();
  }

  public void addStorageGroup(String storageGroup) {
    ownedStorageGroups.add(storageGroup);
  }

  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public List<String> getOwnedStorageGroups() {
    return ownedStorageGroups;
  }
}
