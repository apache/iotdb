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

package com.timecho.iotdb.confignode.persistence.executor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.quota.DeleteUserResourceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetUserResourceQuotaPlan;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;

/** Executes TimechoDB-only ConfigNode physical plans that need dedicated handling. */
public final class TimechoConfigPlanExecutor {

  private TimechoConfigPlanExecutor() {}

  /**
   * Plans routed here must not appear as case labels in {@code
   * ConfigPlanExecutor.executeNonQueryPlan}; add new Timecho-only plan handling in this class.
   */
  public static boolean isTimechoPlan(final ConfigPhysicalPlanType planType) {
    switch (planType) {
      case setUserResourceQuota:
      case deleteUserResourceQuota:
        return true;
      default:
        return false;
    }
  }

  public static TSStatus executeNonQueryPlan(ConfigPhysicalPlan physicalPlan, QuotaInfo quotaInfo)
      throws UnknownPhysicalPlanTypeException {
    switch (physicalPlan.getType()) {
      case setUserResourceQuota:
        return quotaInfo.setUserResourceQuota((SetUserResourceQuotaPlan) physicalPlan);
      case deleteUserResourceQuota:
        return quotaInfo.deleteUserResourceQuota((DeleteUserResourceQuotaPlan) physicalPlan);
      default:
        throw new UnknownPhysicalPlanTypeException(physicalPlan.getType());
    }
  }
}
