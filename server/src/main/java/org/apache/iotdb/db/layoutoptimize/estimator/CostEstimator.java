package org.apache.iotdb.db.layoutoptimize.estimator;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.layoutoptimize.DataSizeInfoNotExistsException;
import org.apache.iotdb.db.exception.layoutoptimize.SampleRateNoExistsException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.layoutoptimize.diskevaluate.DiskEvaluator;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;

import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CostEstimator {
  private DiskEvaluator.DiskInfo diskInfo = DiskEvaluator.getInstance().getDiskInfo();
  private static final CostEstimator INSTANCE = new CostEstimator();

  private CostEstimator() {}

  public static CostEstimator getInstance() {
    return INSTANCE;
  }

  /**
   * Estimate the cost of a query according to the modeling of query process in IoTDB
   *
   * @param query the query to be estimated
   * @param physicalOrder the physical order of chunk in tsfile
   * @param chunkSize the average chunk size in disk
   * @return the cost in milliseconds
   */
  public double estimate(QueryRecord query, List<String> physicalOrder, long chunkSize) {
    try {
      MManager metadataManager = MManager.getInstance();
      String storageGroup = metadataManager.getStorageGroupPath(query.getDevice()).getFullPath();
      long dataPoint = DataSizeEstimator.getInstance().getPointNumInDisk(storageGroup, chunkSize);
      double maxSampleRate = -1;
      SampleRateKeeper sampleRateKeeper = SampleRateKeeper.getInstance();
      if (!sampleRateKeeper.hasSampleRateForDevice(query.getDevice().getFullPath())) {
        try {
          sampleRateKeeper.updateSampleRate(query.getDevice().getFullPath());
        } catch (QueryProcessException
            | TException
            | StorageEngineException
            | SQLException
            | IOException
            | InterruptedException
            | QueryFilterOptimizationException
            | MetadataException e) {
          e.printStackTrace();
          return -1;
        }
      }
      for (String measurement : query.getMeasurements()) {
        maxSampleRate =
            Math.max(
                maxSampleRate,
                sampleRateKeeper.getSampleRate(query.getDevice().getFullPath(), measurement));
      }
      long visitPointNum = (long) (query.getSpan() * maxSampleRate);
      int chunkGroupNum = (int) (visitPointNum / dataPoint + 1);
      double readCost =
          ((double) chunkSize)
              * query.getMeasurements().size()
              / diskInfo.readSpeed
              * chunkGroupNum;
      Set<String> measurements = new HashSet<>(query.getMeasurements());
      int firstMeasurementPos = -1;
      for (int i = 0; i < physicalOrder.size(); i++) {
        if (measurements.contains(physicalOrder.get(i))) {
          firstMeasurementPos = i;
          break;
        }
      }
      double initSeekCost = getSeekCost(firstMeasurementPos * chunkSize);
      double intermediateSeekCost = 0.0d;
      int seekCount = 0;
      int lastIdx = 0;
      for (int i = firstMeasurementPos + 1; i < physicalOrder.size(); i++) {
        if (measurements.contains(physicalOrder.get(i))) {
          intermediateSeekCost += getSeekCost(chunkSize * seekCount);
          seekCount = 0;
          lastIdx = i;
        } else {
          seekCount++;
        }
      }
      double fromLastToFirstSeek =
          getSeekCost((physicalOrder.size() - lastIdx - 1 + firstMeasurementPos) * chunkSize);
      intermediateSeekCost += fromLastToFirstSeek;
      intermediateSeekCost *= chunkGroupNum;
      return intermediateSeekCost + initSeekCost + readCost;
    } catch (DataSizeInfoNotExistsException
        | SampleRateNoExistsException
        | StorageGroupNotSetException e) {
      e.printStackTrace();
      return -1L;
    }
  }

  private double getSeekCost(long seekDistance) {
    if (seekDistance == 0) {
      return 0;
    }
    if (seekDistance < diskInfo.seekDistance.get(0)) {
      return (double) seekDistance
          / (double) diskInfo.seekDistance.get(0)
          * diskInfo.seekCost.get(0);
    }
    for (int i = 0; i < diskInfo.seekDistance.size() - 1; i++) {
      if (seekDistance >= diskInfo.seekDistance.get(i)
          && seekDistance < diskInfo.seekDistance.get(i + 1)) {
        double deltaX = seekDistance - diskInfo.seekDistance.get(i);
        double deltaY = diskInfo.seekCost.get(i + 1) - diskInfo.seekCost.get(i);
        return deltaX
                / ((double) diskInfo.seekDistance.get(i + 1) - diskInfo.seekDistance.get(i))
                * deltaY
            + diskInfo.seekCost.get(i);
      }
    }
    return -1.0d;
  }

  /**
   * Estimate the cost of a list of queries
   *
   * @param records the list of queries
   * @param physicalOrder the physical order of chunk in tsfile
   * @param averageChunkSize the average chunk size
   * @return the cost in milliseconds
   */
  public double estimate(
      List<QueryRecord> records, List<String> physicalOrder, long averageChunkSize) {
    double totalCost = 0;
    for (QueryRecord record : records) {
      totalCost += estimate(record, physicalOrder, averageChunkSize);
    }
    return totalCost;
  }
}
