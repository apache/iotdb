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
package org.apache.iotdb.cluster.config;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.utils.TestOnly;

public class ClusterConstant {

  /**
   * We only change the value in tests to reduce test time, so they are essentially constant. A
   * failed election will restart in [0, max(heartbeatInterval, 50ms)). If this range is too small,
   * a stale node may frequently issue elections and thus makes the leader step down.
   */
  private static long electionMaxWaitMs =
      Math.max(ClusterDescriptor.getInstance().getConfig().getHeartbeatIntervalMs(), 50L);

  public static final int SLOT_NUM = 10000;
  public static final int HASH_SALT = 2333;
  public static final int CHECK_ALIVE_TIME_OUT_MS = 1000;

  public static final int LOG_NUM_IN_BATCH = 100;

  public static final int RETRY_WAIT_TIME_MS = 10;

  public static final int THREAD_POLL_WAIT_TERMINATION_TIME_S = 10;

  public static final Node EMPTY_NODE = new Node();

  private ClusterConstant() {
    // constant class
  }

  public static long getElectionMaxWaitMs() {
    return electionMaxWaitMs;
  }

  @TestOnly
  public static void setElectionMaxWaitMs(long electionMaxWaitMs) {
    ClusterConstant.electionMaxWaitMs = electionMaxWaitMs;
  }
}
