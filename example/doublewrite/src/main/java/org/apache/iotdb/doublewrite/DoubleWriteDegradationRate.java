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

/**
 * This is a double write insertion degradation rate test java class, which shows the performance
 * impact when enable double write feature. You can run this code in the same way as
 * DoubleWriteExample.java. Since IoTDB-A enable the double write feature, the performance impact is
 * correct only when Both IoTDB-A and IoTDB-B run on the same computer. Or you can modify the
 * default configuration of IoTDB-A and IoTDB-B after becoming familiar with DoubleWriteExample,
 * DoubleWriteUtil to get A more accurate performance impact estimate from two remote computers.
 */
public class DoubleWriteDegradationRate extends DoubleWriteUtil {

  private static final String dA = "d0";
  private static final String dB = "d1";

  /**
   * The following three fields are insert configuration parameters. The double write feature
   * already applies to all write interfaces, so you are free to modify these parameters.
   */
  // Total insertion requests during test
  private static final int batchCnt = 10000;
  // The insertion timeseries count per timestamp
  private static final int timeseriesCnt = 100;
  // The insertion rows count per request
  private static final int batchSize = 10;

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
    double doubleWriteCost = System.currentTimeMillis() - startTime;

    startTime = System.currentTimeMillis();
    threadB.start();
    threadB.join();
    double normalWriteCost = System.currentTimeMillis() - startTime;

    // compute performance
    double total = batchCnt * batchSize;
    System.out.println("Normal write cost: " + normalWriteCost / 1000.0 + "s");
    System.out.println("Average: " + normalWriteCost / total + " ms per insertion");
    System.out.println("Double write cost: " + doubleWriteCost / 1000.0 + "s");
    System.out.println("Average: " + doubleWriteCost / total + " ms per insertion");
    System.out.println(
        "Performance degradation rate : "
            + (doubleWriteCost - normalWriteCost) / normalWriteCost * 100.0
            + "%");
  }
}
