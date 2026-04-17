/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.utils.hint;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FollowerHint extends ReplicaHint {
  public static String hintName = "follower";
  private final String table;
  private final List<Integer> nodeIds;

  public FollowerHint(String table, List<Integer> nodeIds) {
    super(hintName);
    this.table = table;
    this.nodeIds = nodeIds;
  }

  @Override
  public String getKey() {
    return category + "-" + table;
  }

  @Override
  public String toString() {
    return hintName + "-" + table;
  }

  @Override
  public List<TDataNodeLocation> selectLocations(List<TDataNodeLocation> dataNodeLocations) {
    if (dataNodeLocations == null || dataNodeLocations.size() <= 1) {
      return dataNodeLocations;
    }

    List<TDataNodeLocation> followerLocations =
        dataNodeLocations.subList(1, dataNodeLocations.size());
    if (nodeIds == null || nodeIds.isEmpty()) {
      return followerLocations;
    }

    // nodeId -> Location for O(1) lookup
    Map<Integer, TDataNodeLocation> followerLocationMap = new HashMap<>();
    for (TDataNodeLocation location : followerLocations) {
      followerLocationMap.put(location.getDataNodeId(), location);
    }

    // Find the first nodeId that exists in followerLocations and return its location
    for (Integer nodeId : nodeIds) {
      TDataNodeLocation location = followerLocationMap.get(nodeId);
      if (location != null) {
        return Collections.singletonList(location);
      }
    }

    // If no matching nodeId found, return all follower locations
    return followerLocations;
  }
}
