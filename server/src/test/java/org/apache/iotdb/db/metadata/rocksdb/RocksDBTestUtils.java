package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;

import java.util.Collection;
import java.util.List;

public class RocksDBTestUtils {

  public static int WRITE_CLIENT_NUM = 200;
  public static int READ_CLIENT_NUM = 50;

  public static void printBenchmarkBaseline(
      List storageGroups,
      List<List<CreateTimeSeriesPlan>> timeSeriesSet,
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

  public static void printReport(List<RocksDBTestTask.BenchmarkResult> results, String category) {
    System.out.println(
        String.format(
            "\n\n#################################%s benchmark statistics#################################",
            category));
    System.out.println(String.format("%25s %15s %10s %15s", "", "success", "fail", "cost-in-ms"));
    for (RocksDBTestTask.BenchmarkResult result : results) {
      System.out.println(
          String.format(
              "%25s %15d %10d %15d",
              result.name, result.successCount, result.failCount, result.costInMs));
    }
  }
}
