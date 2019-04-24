/**
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
package org.apache.iotdb.cluster.rpc.raft.response;

import java.util.Map;

public class QueryMetricResponse extends BasicResponse {

  private Map<String, Integer> value;

  private QueryMetricResponse(String groupId, boolean redirected, String leaderStr,
      String errorMsg) {
    super(groupId, redirected, leaderStr, errorMsg);
  }

  public static QueryMetricResponse createSuccessResponse(String groupId, Map<String, Integer> value) {
    QueryMetricResponse response = new QueryMetricResponse(groupId, false, null,
        null);
    response.value = value;
    return response;
  }

  public static QueryMetricResponse createErrorResponse(String groupId, String errorMsg) {
    return new QueryMetricResponse(groupId, false, null, errorMsg);
  }

  public Map<String, Integer> getValue() {
    return value;
  }
}
