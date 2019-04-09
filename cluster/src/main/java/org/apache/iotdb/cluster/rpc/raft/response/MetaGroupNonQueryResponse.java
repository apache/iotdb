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

/**
 * Handle response from metadata group leader
 */
public class MetaGroupNonQueryResponse extends BasicResponse {

  private MetaGroupNonQueryResponse(String groupId, boolean redirected, String leaderStr,
      String errorMsg) {
    super(groupId, redirected, leaderStr, errorMsg);
  }

  public static MetaGroupNonQueryResponse createRedirectedResponse(String groupId, String leaderStr) {
    return new MetaGroupNonQueryResponse(groupId, true, leaderStr, null);
  }

  public static MetaGroupNonQueryResponse createEmptyResponse(String groupId) {
    return new MetaGroupNonQueryResponse(groupId, false, null, null);
  }

}
