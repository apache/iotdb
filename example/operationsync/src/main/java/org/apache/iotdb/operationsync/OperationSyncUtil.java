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
package org.apache.iotdb.operationsync;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

/**
 * This java class is used to create the operation sync examples environment. You can set IoTDB-B
 * config here
 */
public abstract class OperationSyncUtil {

  // IoTDB-A config
  // Started by EnvironmentUtils, shouldn't be modified
  protected static final String ipA = "127.0.0.1";
  protected static final int portA = 6667;
  protected static final String userA = "root";
  protected static final String passwordA = "root";

  // IoTDB-B config
  // You can modify that config in order to connect with IoTDB-B you started
  protected static final String ipB = "127.0.0.1";
  protected static final int portB = 6668;
  protected static final String userB = "root";
  protected static final String passwordB = "root";

  protected static SessionPool sessionPoolA;
  protected static SessionPool sessionPoolB;
  // The sessionPool concurrency
  protected static final int concurrency = 5;

  // Default name of StorageGroup
  protected static final String sg = "root.OPERATIONSYNCSG";

  // Threads for operation sync
  protected static Thread threadA;
  protected static Thread threadB;

  protected static void initEnvironment() {
    // Start local IoTDB-A on ip "127.0.0.1", port 6667 and set enableOperationSync
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

    config.setEnableOperationSync(true);
    config.setSecondaryAddress(ipB);
    config.setSecondaryPort(portB);
    config.setSecondaryUser(userB);
    config.setSecondaryPassword(passwordB);
    config.setOperationSyncMaxLogSize(1024);

    EnvironmentUtils.envSetUp();
  }

  protected static void initSessionPool(
      String dA, String dB, int batchCnt, int timeseriesCnt, int batchSize)
      throws IoTDBConnectionException, StatementExecutionException {
    // Create sessionPools
    sessionPoolA = new SessionPool(ipA, portA, userA, passwordA, concurrency);
    sessionPoolB = new SessionPool(ipB, portB, userB, passwordB, concurrency);

    // Create StorageGroups
    try {
      sessionPoolA.deleteStorageGroup(sg);
    } catch (Exception ignored) {
      // ignored
    }
    try {
      sessionPoolB.deleteStorageGroup(sg);
    } catch (Exception ignored) {
      // ignored
    }
    sessionPoolA.setStorageGroup(sg);
    sessionPoolB.setStorageGroup(sg);

    // Create operation sync threads
    OperationSyncThread operationSyncThreadA =
        new OperationSyncThread(sessionPoolA, dA, batchCnt, timeseriesCnt, batchSize);
    threadA = new Thread(operationSyncThreadA);
    OperationSyncThread operationSyncThreadB =
        new OperationSyncThread(sessionPoolB, dB, batchCnt, timeseriesCnt, batchSize);
    threadB = new Thread(operationSyncThreadB);
  }

  protected static void cleanEnvironment() throws Exception {
    // Clean StorageGroups, close sessionPools and shut down environment
    sessionPoolA.deleteStorageGroup(sg);
    sessionPoolB.deleteStorageGroup(sg);

    sessionPoolA.close();
    sessionPoolB.close();

    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.shutdownDaemon();
  }
}
