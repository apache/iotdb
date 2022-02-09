package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class MRocksDBTest {
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private MRocksDBManager rocksDBManager;

  public MRocksDBTest(MRocksDBManager rocksDBManager) {
    this.rocksDBManager = rocksDBManager;
  }

  public List<RocksDBTestTask.BenchmarkResult> benchmarkResults = new ArrayList<>();

  public void testStorageGroupCreation(List<SetStorageGroupPlan> storageGroups) {
    RocksDBTestTask<SetStorageGroupPlan> task =
        new RocksDBTestTask<>(storageGroups, RocksDBTestUtils.WRITE_CLIENT_NUM, 100);
    RocksDBTestTask.BenchmarkResult result =
        task.runWork(
            setStorageGroupPlan -> {
              try {
                rocksDBManager.setStorageGroup(setStorageGroupPlan.getPath());
                return true;
              } catch (Exception e) {
                return false;
              }
            },
            "CreateStorageGroup");
    benchmarkResults.add(result);
  }

  public void testTimeSeriesCreation(List<List<CreateTimeSeriesPlan>> timeSeriesSet)
      throws IOException {
    RocksDBTestTask<List<CreateTimeSeriesPlan>> task =
        new RocksDBTestTask<>(timeSeriesSet, RocksDBTestUtils.WRITE_CLIENT_NUM, 100);
    RocksDBTestTask.BenchmarkResult result =
        task.runBatchWork(
            createTimeSeriesPlans -> {
              RocksDBTestTask.TaskResult taskResult = new RocksDBTestTask.TaskResult();
              createTimeSeriesPlans.stream()
                  .forEach(
                      ts -> {
                        try {
                          rocksDBManager.createTimeseries(
                              ts.getPath(),
                              ts.getDataType(),
                              ts.getEncoding(),
                              ts.getCompressor(),
                              ts.getProps(),
                              ts.getAlias());
                          taskResult.success++;
                        } catch (Exception e) {
                          e.printStackTrace();
                          taskResult.failure++;
                        }
                      });
              return taskResult;
            },
            "CreateTimeSeries");
    benchmarkResults.add(result);
  }

  //  public void testMeasurementNodeQuery(Collection<String> queryTsSet) {
  //    RocksDBTestTask<String> task =
  //        new RocksDBTestTask<>(queryTsSet, RocksDBTestUtils.WRITE_CLIENT_NUM, 10000);
  //    RocksDBTestTask.BenchmarkResult result =
  //        task.runWork(
  //            s -> {
  //              try {
  //                IMNode node = rocksDBManager.getMeasurementMNode(new PartialPath(s));
  //                if (node != null) {
  //                  node.toString();
  //                }
  //                return true;
  //              } catch (Exception e) {
  //                return false;
  //              }
  //            },
  //            "MeasurementNodeQuery");
  //    benchmarkResults.add(result);
  //  }

  public void testNodeChildrenQuery(Collection<String> queryTsSet) {
    RocksDBTestTask<String> task =
        new RocksDBTestTask<>(queryTsSet, RocksDBTestUtils.WRITE_CLIENT_NUM, 10000);
    RocksDBTestTask.BenchmarkResult result =
        task.runWork(
            s -> {
              try {
                Set<String> children =
                    rocksDBManager.getChildNodePathInNextLevel(new PartialPath(s));
                if (children != null) {
                  return true;
                } else {
                  return false;
                }
              } catch (Exception e) {
                return false;
              }
            },
            "NodeChildrenQuery");
    benchmarkResults.add(result);
  }

  public void testLevelScan() throws MetadataException {
    long start = System.currentTimeMillis();
    List<PartialPath> level1 = rocksDBManager.getNodesListInGivenLevel(null, 1);
    List<PartialPath> level2 = rocksDBManager.getNodesListInGivenLevel(null, 2);
    List<PartialPath> level3 = rocksDBManager.getNodesListInGivenLevel(null, 3);
    List<PartialPath> level4 = rocksDBManager.getNodesListInGivenLevel(null, 4);
    List<PartialPath> level5 = rocksDBManager.getNodesListInGivenLevel(null, 5);
    long totalCount = level1.size() + level2.size() + level3.size() + level4.size() + level5.size();
    RocksDBTestTask.BenchmarkResult result =
        new RocksDBTestTask.BenchmarkResult(
            "levelScan", totalCount, 0, System.currentTimeMillis() - start);
    benchmarkResults.add(result);
  }

  //  public void testCountTimeseries() throws MetadataException {
  //    long start = System.currentTimeMillis();
  //    int count = rocksDBManager.getAllTimeseriesCount(new PartialPath("root.**"));
  //    RocksDBTestTask.BenchmarkResult result =
  //        new RocksDBTestTask.BenchmarkResult(
  //            "timeseriesCount", count, 0, System.currentTimeMillis() - start);
  //    benchmarkResults.add(result);
  //  }

  //  public void testCountTimeseriesByTable() {
  //    long start = System.currentTimeMillis();
  //    long count = rocksDBManager.countMeasurementNodes();
  //    RocksDBTestTask.BenchmarkResult result =
  //        new RocksDBTestTask.BenchmarkResult(
  //            "traverseAllMeasurement", count, 0, System.currentTimeMillis() - start);
  //    benchmarkResults.add(result);
  //  }
  //
  //  public void testCountStorageGroupByTable() {
  //    long start = System.currentTimeMillis();
  //    long count = rocksDBManager.countStorageGroupNodes();
  //    RocksDBTestTask.BenchmarkResult result =
  //        new RocksDBTestTask.BenchmarkResult(
  //            "storageGroupCountByTable", count, 0, System.currentTimeMillis() - start);
  //    benchmarkResults.add(result);
  //  }
  //
  //  public void testCountDeviceByTable() {
  //    long start = System.currentTimeMillis();
  //    long count = rocksDBManager.countDeviceNodes();
  //    RocksDBTestTask.BenchmarkResult result =
  //        new RocksDBTestTask.BenchmarkResult(
  //            "traverseAllDevice", count, 0, System.currentTimeMillis() - start);
  //    benchmarkResults.add(result);
  //  }
  //
  //  public void testCountDevices() throws MetadataException {
  //    long start = System.currentTimeMillis();
  //    int count = rocksDBManager.getDevicesNum(new PartialPath("root.**"));
  //    RocksDBTestTask.BenchmarkResult result =
  //        new RocksDBTestTask.BenchmarkResult(
  //            "devicesCount", count, 0, System.currentTimeMillis() - start);
  //    benchmarkResults.add(result);
  //  }
}
