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

public class ReplicaHint extends Hint {
  public static String HINT_NAME = "replica";

  private @Nullable final QualifiedName table;
  private final int replicaIndex;

  public ReplicaHint(@Nullable QualifiedName table, int replicaIndex) {
    super(HINT_NAME);
    this.table = table;
    this.replicaIndex = replicaIndex;
  }

  @Override
  public String getKey() {
    return HINT_NAME + (table == null ? "" : "-" + table);
  }

  @Override
  public String toString() {
    return HINT_NAME + (table == null ? "" : "-" + table) + "(" + replicaIndex + ")";
  }

  /**
   * Selects data node locations based on the replica strategy.
   *
   * @param dataNodeLocations the available data node locations
   * @return the selected locations based on replica hint strategy
   */
  public List<TDataNodeLocation> selectLocations(List<TDataNodeLocation> dataNodeLocations) {
    if (dataNodeLocations == null || dataNodeLocations.isEmpty()) {
      return null;
    }
    return ImmutableList.of(dataNodeLocations.get(replicaIndex % dataNodeLocations.size()));
  }
}
