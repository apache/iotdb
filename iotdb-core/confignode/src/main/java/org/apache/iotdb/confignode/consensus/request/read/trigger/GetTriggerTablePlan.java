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

package org.apache.iotdb.confignode.consensus.request.read.trigger;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Objects;

public class GetTriggerTablePlan extends ConfigPhysicalReadPlan {
  private final boolean onlyStateful;

  public GetTriggerTablePlan(final boolean onlyStateful) {
    super(ConfigPhysicalPlanType.GetTriggerTable);
    this.onlyStateful = onlyStateful;
  }

  public boolean isOnlyStateful() {
    return onlyStateful;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final GetTriggerTablePlan that = (GetTriggerTablePlan) o;
    return onlyStateful == that.onlyStateful;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), onlyStateful);
  }
}
