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
package org.apache.iotdb.library.util;

import java.io.IOException;

/** For data generation using IoTDB benchmark for 0.12.0. */
public class DataGeneration_0_12_0 {
  public static void generateData(String scriptType) throws IOException, InterruptedException {
    if (scriptType.equalsIgnoreCase("cmd")) {
      Runtime run = Runtime.getRuntime();
      run.exec("src\\test\\resources\\iotdb-0.12-0.0.1\\benchmark.bat");
      Thread.sleep(10000);
      System.out.println("Generation succeeded.");
    } else if (scriptType.equalsIgnoreCase("bash")) {
      Runtime run = Runtime.getRuntime();
      run.exec("sh src/test/resources/iotdb-0.12-0.0.1/benchmark.sh");
      Thread.sleep(10000);
      System.out.println("Generation succeeded.");
    }
  }
}
