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
package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;

import java.util.Collection;
import java.util.List;

public class RocksDBTestUtils {

  public static int WRITE_CLIENT_NUM = 200;
  public static int READ_CLIENT_NUM = 50;

  public static void printBenchmarkBaseline(
      List storageGroups,
      List<List<ICreateTimeSeriesPlan>> timeSeriesSet,
      Collection queryTsSet,
      Collection innerPathSet) {
    System.out.println(
        "#################################Benchmark configuration#################################");
    System.out.println("-----------Configuration---------");
    System.out.println("Write client num: " + WRITE_CLIENT_NUM);
    System.out.println("Query client num: " + READ_CLIENT_NUM);
    System.out.println("-----------Benchmark Data Set Size---------");
    System.out.println("StorageGroup: " + storageGroups.size());
    int count = 0;
    for (List l : timeSeriesSet) {
      count += l.size();
    }
    System.out.println("TimeSeries: " + count);
    System.out.println("MeasurementNodeQuery: " + queryTsSet.size());
    System.out.println("ChildrenNodeQuery: " + innerPathSet.size());
  }

  public static void printMemInfo(String stageInfo) {
    System.out.printf(
        "[%s] Memory used: %d%n",
        stageInfo, Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
  }

  public static void printReport(
      List<RocksDBBenchmarkTask.BenchmarkResult> results, String category) {
    System.out.println(
        String.format(
            "\n\n#################################%s benchmark statistics#################################",
            category));
    System.out.println(String.format("%25s %15s %10s %15s", "", "success", "fail", "cost-in-ms"));
    for (RocksDBBenchmarkTask.BenchmarkResult result : results) {
      System.out.println(
          String.format(
              "%25s %15d %10d %15d",
              result.name, result.successCount, result.failCount, result.costInMs));
    }
  }
}
