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
import java.util.List;

public class LeaderHint extends ReplicaHint {
  public static String hintName = "leader";
  private final String targetTable;

  public LeaderHint(List<String> tables) {
    super(hintName);
    if (tables == null || tables.size() > 1) {
      throw new IllegalArgumentException("LeaderHint accepts empty or exactly one table");
    }
    targetTable = tables.isEmpty() ? "*" : tables.get(0);
  }

  @Override
  public String getKey() {
    return category + "-" + targetTable;
  }

  @Override
  public String toString() {
    return hintName + "-" + targetTable;
  }

  @Override
  public List<TDataNodeLocation> selectLocations(List<TDataNodeLocation> dataNodeLocations) {
    if (dataNodeLocations == null || dataNodeLocations.size() <= 1) {
      return dataNodeLocations;
    }
    // Return only the leader (first location)
    return Collections.singletonList(dataNodeLocations.get(0));
  }
}
