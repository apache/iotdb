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

import java.util.List;

/**
 * Abstract base class for replica-related hints. Provides common functionality for hints that deal
 * with data node replica selection.
 */
public abstract class ReplicaHint extends Hint {
  public static String category = "replica";

  protected ReplicaHint(String hintName) {
    super(hintName, category);
  }

  /**
   * Selects data node locations based on the replica strategy. Each replica hint implementation
   * defines its own location selection logic.
   *
   * @param dataNodeLocations the available data node locations
   * @return the selected locations based on replica hint strategy
   */
  public abstract List<TDataNodeLocation> selectLocations(
      List<TDataNodeLocation> dataNodeLocations);
}
