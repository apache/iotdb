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
package org.apache.iotdb.commons.loadbalance;

import java.io.IOException;

public enum LeaderDistributionPolicy {
  GREEDY("GREEDY"),
  MIN_COST_FLOW("MIN_COST_FLOW");

  private final String policy;

  LeaderDistributionPolicy(String policy) {
    this.policy = policy;
  }

  public String getPolicy() {
    return policy;
  }

  public static LeaderDistributionPolicy parse(String policy) throws IOException {
    for (LeaderDistributionPolicy leaderDistributionPolicy : LeaderDistributionPolicy.values()) {
      if (leaderDistributionPolicy.policy.equals(policy)) {
        return leaderDistributionPolicy;
      }
    }
    throw new IOException(String.format("LeaderDistributionPolicy %s doesn't exist.", policy));
  }
}
