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

package org.apache.iotdb.consensus.multileader.conf;

import java.util.concurrent.TimeUnit;

// TODO make it configurable
public class MultiLeaderConsensusConfig {

  private MultiLeaderConsensusConfig() {}

  public static final int RPC_MAX_CONCURRENT_CLIENT_NUM = 65535;
  public static final int THRIFT_SERVER_AWAIT_TIME_FOR_STOP_SERVICE = 60;
  public static final boolean IS_RPC_THRIFT_COMPRESSION_ENABLED = false;
  public static final int SELECTOR_NUM_OF_CLIENT_MANAGER = 1;
  public static final int CONNECTION_TIMEOUT_IN_MS = (int) TimeUnit.SECONDS.toMillis(20);
  public static final int MAX_PENDING_REQUEST_NUM_PER_NODE = 1000;
  public static final int MAX_REQUEST_PER_BATCH = 100;
  public static final int MAX_PENDING_BATCH = 50;
  public static final int MAX_WAITING_TIME_FOR_ACCUMULATE_BATCH_IN_MS = 10;
  public static final long BASIC_RETRY_WAIT_TIME_MS = TimeUnit.MILLISECONDS.toMillis(100);
  public static final long MAX_RETRY_WAIT_TIME_MS = TimeUnit.SECONDS.toMillis(20);
}
