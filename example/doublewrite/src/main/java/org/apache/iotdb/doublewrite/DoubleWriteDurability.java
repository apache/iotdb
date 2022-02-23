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
package org.apache.iotdb.doublewrite;

import org.apache.iotdb.session.pool.SessionPool;

public class DoubleWriteDurability extends DoubleWriteUtil {

  private static final String dA = "d0";
  private static final String dB = "d1";

  private static final int batchCnt = 3000;
  private static final int timeseriesCnt = 1000;
  private static final int batchSize = 1;

  public static void main(String[] args) throws Exception {
    initEnvironment();

    sessionPoolA = new SessionPool(ipA, portA, userA, passwordA, concurrency);
    // Create StorageGroups
    try {
      sessionPoolA.deleteStorageGroup(sg);
    } catch (Exception ignored) {
      // ignored
    }
    sessionPoolA.setStorageGroup(sg);

    // Create double write threads
    DoubleWriteThread doubleWriteThreadA =
        new DoubleWriteThread(sessionPoolA, dA, batchCnt, timeseriesCnt, batchSize);
    threadA = new Thread(doubleWriteThreadA);
    DoubleWriteThread doubleWriteThreadB =
        new DoubleWriteThread(sessionPoolA, dB, batchCnt, timeseriesCnt, batchSize);
    threadB = new Thread(doubleWriteThreadB);

    insertData();
  }

  private static void insertData() throws InterruptedException {
    threadA.start();
    threadB.start();

    threadA.join();
    threadB.join();

    System.out.println("Insertion complete.");
  }
}
