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

package org.apache.iotdb.db.queryengine.plan.relational.utils.hint;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class RegionRouteHint extends Hint {
  public static String HINT_NAME = "region_route";
  public static int ANY_TABLE = -1;

  private @Nullable final QualifiedName table;
  private final Map<Integer, Integer> regionDatanodeMap;

  public RegionRouteHint(@Nullable QualifiedName table, Map<Integer, Integer> regionDatanodeMa) {
    super(HINT_NAME);
    this.table = table;
    this.regionDatanodeMap = regionDatanodeMa;
  }

  @Override
  public String getKey() {
    return HINT_NAME + (table == null ? "" : "-" + table);
  }

  @Override
  public String toString() {
    return HINT_NAME + (table == null ? "" : "-" + table) + "(" + regionDatanodeMap + ")";
  }

  /**
   * Selects data node locations based on the region_route strategy.
   *
   * @param dataNodeLocations the available data node locations
   * @param regionId the region ID to route
   * @return the selected locations based on region_route hint strategy, or null if no match
   */
  public List<TDataNodeLocation> selectLocations(
      List<TDataNodeLocation> dataNodeLocations, int regionId) {
    if (dataNodeLocations == null || dataNodeLocations.isEmpty()) {
      return null;
    }

    Integer datanodeId = regionDatanodeMap.getOrDefault(regionId, regionDatanodeMap.get(ANY_TABLE));
    if (datanodeId == null) {
      return null;
    }

    return dataNodeLocations.stream()
        .filter(location -> location.getDataNodeId() == datanodeId)
        .findFirst()
        .map(ImmutableList::of)
        .orElse(null);
  }
}
