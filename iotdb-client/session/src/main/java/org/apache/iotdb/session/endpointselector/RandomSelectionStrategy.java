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

package org.apache.iotdb.session.endpointselector;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Random selection strategy that avoids already tried endpoints */
public class RandomSelectionStrategy implements EndpointSelectionStrategy {

  private static final String STRATEGY_NAME = "random";
  private final SecureRandom random = new SecureRandom();

  @Override
  public TEndPoint selectNext(List<TEndPoint> availableEndpoints, Set<TEndPoint> triedEndpoints) {
    if (availableEndpoints == null || availableEndpoints.isEmpty()) {
      return null;
    }

    // Create list of untried endpoints
    List<TEndPoint> untriedEndpoints = new ArrayList<>();
    for (TEndPoint endpoint : availableEndpoints) {
      if (!triedEndpoints.contains(endpoint)) {
        untriedEndpoints.add(endpoint);
      }
    }

    if (untriedEndpoints.isEmpty()) {
      // All endpoints have been tried in this reconnection attempt
      return null;
    }

    // Randomly select from untried endpoints
    return untriedEndpoints.get(random.nextInt(untriedEndpoints.size()));
  }

  @Override
  public String getName() {
    return STRATEGY_NAME;
  }
}
