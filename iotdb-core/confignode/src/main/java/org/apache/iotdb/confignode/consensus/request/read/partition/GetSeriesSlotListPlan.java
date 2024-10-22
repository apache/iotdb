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

package org.apache.iotdb.confignode.consensus.request.read.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Objects;

public class GetSeriesSlotListPlan extends ConfigPhysicalReadPlan {

  private final String database;

  private final TConsensusGroupType partitionType;

  public GetSeriesSlotListPlan(final String database, final TConsensusGroupType partitionType) {
    super(ConfigPhysicalPlanType.GetSeriesSlotList);
    this.database = database;
    this.partitionType = partitionType;
  }

  public String getDatabase() {
    return database;
  }

  public TConsensusGroupType getPartitionType() {
    return partitionType;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GetSeriesSlotListPlan that = (GetSeriesSlotListPlan) o;
    return database.equals(that.database) && partitionType.equals(that.partitionType);
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode = hashcode * 31 + Objects.hash(database);
    hashcode = hashcode * 31 + Objects.hash(partitionType.ordinal());
    return hashcode;
  }
}
