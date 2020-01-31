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

  public static final long RESPONSE_UNSET = 0;
  public static final long RESPONSE_AGREE = -1;
  public static final long RESPONSE_LOG_MISMATCH = -2;
  public static final long RESPONSE_REJECT = -3;
  public static final long RESPONSE_PARTITION_TABLE_UNAVAILABLE = -4;
  public static final long RESPONSE_IDENTIFIER_CONFLICT = -5;
  public static final long RESPONSE_NO_CONNECTION = -6;
  public static final long RESPONSE_META_LOG_STALE = -7;
  public static final long RESPONSE_LEADER_STILL_ONLINE = -8;

  private Response() {
    // enum class
  }

}
