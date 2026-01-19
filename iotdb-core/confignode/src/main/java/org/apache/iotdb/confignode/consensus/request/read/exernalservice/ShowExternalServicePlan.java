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

package org.apache.iotdb.confignode.consensus.request.read.exernalservice;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Objects;
import java.util.Set;

/** Get infos of ExternalService by the DataNode's id. */
public class ShowExternalServicePlan extends ConfigPhysicalReadPlan {

  private final Set<Integer> dataNodeIds;

  public ShowExternalServicePlan(Set<Integer> dataNodeIds) {
    super(ConfigPhysicalPlanType.ShowExternalService);
    this.dataNodeIds = dataNodeIds;
  }

  public Set<Integer> getDataNodeIds() {
    return dataNodeIds;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ShowExternalServicePlan that = (ShowExternalServicePlan) o;
    return dataNodeIds.equals(that.dataNodeIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataNodeIds);
  }
}
