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

import java.util.List;
import java.util.Set;

/** Sequential selection strategy that cycles through endpoints in order */
public class SequentialSelectionStrategy implements EndpointSelectionStrategy {

  private static final String STRATEGY_NAME = "sequential";
  private int currentIndex = 0;

  @Override
  public TEndPoint selectNext(List<TEndPoint> availableEndpoints, Set<TEndPoint> triedEndpoints) {
    if (availableEndpoints == null || availableEndpoints.isEmpty()) {
      return null;
    }

    // Find next untried endpoint starting from current index
    for (int attempt = 0; attempt < availableEndpoints.size(); attempt++) {
      int index = (currentIndex + attempt) % availableEndpoints.size();
      TEndPoint candidate = availableEndpoints.get(index);

      if (!triedEndpoints.contains(candidate)) {
        currentIndex = (index + 1) % availableEndpoints.size();
        return candidate;
      }
    }

    // All endpoints have been tried in this reconnection attempt
    return null;
  }

  @Override
  public void reset() {
    currentIndex = 0;
  }

  @Override
  public String getName() {
    return STRATEGY_NAME;
  }
}
