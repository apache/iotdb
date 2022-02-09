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

  // Heartbeat client connection timeout should not be larger than heartbeat interval, otherwise
  // the thread pool of sending heartbeats or requesting votes may be used up by waiting for
  // establishing connection with some slow or dead nodes.
  private static final int heartbeatClientConnTimeoutMs =
      Math.min(
          (int) ClusterConstant.getHeartbeatIntervalMs(),
          ClusterConstant.getConnectionTimeoutInMS());

  public static final int SLOT_NUM = 10000;
  public static final int HASH_SALT = 2333;
  public static final int CHECK_ALIVE_TIME_OUT_MS = 1000;

  public static final int LOG_NUM_IN_BATCH = 100;

  public static final int RETRY_WAIT_TIME_MS = 10;

  public static final int THREAD_POLL_WAIT_TERMINATION_TIME_S = 10;

  /**
   * every "REPORT_INTERVAL_SEC" seconds, a reporter thread will print the status of all raft
   * members in this node.
   */
  public static final int REPORT_INTERVAL_SEC = 10;

  /**
   * during snapshot, hardlinks of data files are created to for downloading. hardlinks will be
   * checked every hour by default to see if they have expired, and will be cleaned if so.
   */
  public static final long CLEAN_HARDLINK_INTERVAL_SEC = 3600L;

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

  private static int connectionTimeoutInMS =
      ClusterDescriptor.getInstance().getConfig().getConnectionTimeoutInMS();
  private static int readOperationTimeoutMS =
      ClusterDescriptor.getInstance().getConfig().getReadOperationTimeoutMS();
  private static int writeOperationTimeoutMS =
      ClusterDescriptor.getInstance().getConfig().getWriteOperationTimeoutMS();
  private static int syncLeaderMaxWaitMs = 20 * 1000;
  private static long heartbeatIntervalMs =
      ClusterDescriptor.getInstance().getConfig().getHeartbeatIntervalMs();
  private static long electionTimeoutMs =
      ClusterDescriptor.getInstance().getConfig().getElectionTimeoutMs();

  public static int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public static void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    ClusterConstant.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public static int getReadOperationTimeoutMS() {
    return readOperationTimeoutMS;
  }

  public static int getWriteOperationTimeoutMS() {
    return writeOperationTimeoutMS;
  }

  public static int getSyncLeaderMaxWaitMs() {
    return syncLeaderMaxWaitMs;
  }

  public static void setSyncLeaderMaxWaitMs(int syncLeaderMaxWaitMs) {
    ClusterConstant.syncLeaderMaxWaitMs = syncLeaderMaxWaitMs;
  }

  public static long getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  public static void setHeartbeatIntervalMs(long heartBeatIntervalMs) {
    ClusterConstant.heartbeatIntervalMs = heartBeatIntervalMs;
  }

  public static long getElectionTimeoutMs() {
    return electionTimeoutMs;
  }

  public static void setElectionTimeoutMs(long electionTimeoutMs) {
    ClusterConstant.electionTimeoutMs = electionTimeoutMs;
  }

  public static int getHeartbeatClientConnTimeoutMs() {
    return heartbeatClientConnTimeoutMs;
  }

  @TestOnly
  public static void setReadOperationTimeoutMS(int readOperationTimeoutMS) {
    ClusterConstant.readOperationTimeoutMS = readOperationTimeoutMS;
  }

  @TestOnly
  public static void setWriteOperationTimeoutMS(int writeOperationTimeoutMS) {
    ClusterConstant.writeOperationTimeoutMS = writeOperationTimeoutMS;
  }
}
