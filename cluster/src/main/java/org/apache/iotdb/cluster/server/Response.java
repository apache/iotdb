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

package org.apache.iotdb.cluster.server;

/**
 * Response defines the numeric responses that have special meanings. Enum class is not used for
 * thrift compatibility and to reduce communication cost.
 */
public class Response {

  // the request is successfully accepted
  public static final long RESPONSE_AGREE = -1;
  // cannot find the previous log of the log sent to the follower, a catch-up is required
  public static final long RESPONSE_LOG_MISMATCH = -2;
  // the request is rejected but the detailed reason depends on the type of the request
  public static final long RESPONSE_REJECT = -3;
  // the partition table is not established yet, so related requests are not available
  public static final long RESPONSE_PARTITION_TABLE_UNAVAILABLE = -4;
  // the identifier of the node which wants to join conflicts with one of the joined node's
  public static final long RESPONSE_IDENTIFIER_CONFLICT = -5;
  // the requested node is unreachable in the network
  public static final long RESPONSE_NO_CONNECTION = -6;
  // the node does not give a vote because its leader does not time out. This is to avoid a
  // node which cannot connect to the leader changing the leader in the group frequently.
  public static final long RESPONSE_LEADER_STILL_ONLINE = -7;
  // the operation is rejected because the cluster will not be able to have enough replicas after
  // this operation
  public static final long RESPONSE_CLUSTER_TOO_SMALL = -8;
  // the new node, which tries to join the cluster, contains conflicted parameters with the
  // cluster, so the operation is rejected.
  public static final long RESPONSE_NEW_NODE_PARAMETER_CONFLICT = -9;
  // the data migration of previous add/remove node operations is not finished.
  public static final long RESPONSE_DATA_MIGRATION_NOT_FINISH = -10;
  // the node has removed from the group, so the operation is rejected.
  public static final long RESPONSE_NODE_IS_NOT_IN_GROUP = -11;
  // the request is not executed locally anc should be forwarded
  public static final long RESPONSE_NULL = Long.MIN_VALUE;

  private Response() {
    // enum-like class
  }
}
