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

/**
 * This is an operation sync insertion degradation rate test java class, which shows the performance
 * impact when enable operation sync feature. You can run this code in the same way as
 * OperationSyncExample.java. Since IoTDB-A enable the operation sync feature, the performance
 * impact is correct only when Both IoTDB-A and IoTDB-B run on the same computer. Or you can modify
 * the default configuration of IoTDB-A and IoTDB-B after becoming familiar with
 * OperationSyncExample, OperationSyncUtil to get A more accurate performance impact estimate from
 * two remote computers.
 */
public class OperationSyncDegradationRate extends OperationSyncUtil {

  private static final String dA = "d0";
  private static final String dB = "d1";

  /**
   * The following three fields are insert configuration parameters. The operation sync feature
   * already applies to all write interfaces, so you are free to modify these parameters.
   */
  // Total insertion requests during test
  private static final int batchCnt = 100000;
  // The insertion timeseries count per timestamp
  private static final int timeseriesCnt = 100;
  // The insertion rows count per request
  private static final int batchSize = 1;

  public static void main(String[] args) throws Exception {
    initEnvironment();
    initSessionPool(dA, dB, batchCnt, timeseriesCnt, batchSize);
    insertData();
    cleanEnvironment();
  }

  private static void insertData() throws InterruptedException {
    // do insertion while measuring performance
    long startTime = System.currentTimeMillis();
    threadA.start();
    threadA.join();
    double operationSyncost = System.currentTimeMillis() - startTime;

    startTime = System.currentTimeMillis();
    threadB.start();
    threadB.join();
    double normalWriteCost = System.currentTimeMillis() - startTime;

    // compute performance
    double total = batchCnt * batchSize;
    System.out.println("Normal write cost: " + normalWriteCost / 1000.0 + "s");
    System.out.println("Average: " + normalWriteCost / total + " ms per insertion");
    System.out.println("Operation Sync cost: " + operationSyncost / 1000.0 + "s");
    System.out.println("Average: " + operationSyncost / total + " ms per insertion");
    System.out.println(
        "Performance degradation rate : "
            + (operationSyncost - normalWriteCost) / normalWriteCost * 100.0
            + "%");
  }
}
