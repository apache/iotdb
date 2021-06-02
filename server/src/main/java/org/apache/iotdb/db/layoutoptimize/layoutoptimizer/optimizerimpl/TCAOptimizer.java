package org.apache.iotdb.db.layoutoptimize.layoutoptimizer.optimizerimpl;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.exception.layoutoptimize.DataSizeInfoNotExistsException;
import org.apache.iotdb.db.exception.layoutoptimize.SampleRateNoExistsException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.layoutoptimize.estimator.CostEstimator;
import org.apache.iotdb.db.layoutoptimize.estimator.DataSizeEstimator;
import org.apache.iotdb.db.layoutoptimize.estimator.SampleRateKeeper;
import org.apache.iotdb.db.layoutoptimize.layoutoptimizer.LayoutOptimizer;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

public class TCAOptimizer extends LayoutOptimizer {
  private static final Logger logger = LoggerFactory.getLogger(TCAOptimizer.class);
  private long chunkUpperBound = Long.MIN_VALUE;;
  private long chunkLowerBound = Long.MAX_VALUE;
  private double lowerBoundRatio = 0.5;
  private double upperBoundRatio = 2.0;
  private int preSwapLeft;
  private int preSwapRight;
  private long preChunkSize;
  private int preOperation;

  public TCAOptimizer(PartialPath device) {
    super(device);
  }

  @Override
  public Pair<List<String>, Long> optimize() {
    getChunkBound();
    logger.info("TCA optimizer start with config: {}", config);
    CostEstimator estimator = CostEstimator.getInstance();
    double oriCost = estimator.estimate(records, measurementOrder, averageChunkSize);
    double temperature = config.getSAInitTemperature();
    double coolingRate = config.getSACoolingRate();
    int maxIteration = config.getSAMaxIteration();
    long maxTime = config.getSAMaxTime();
    long startTime = System.currentTimeMillis();
    Random random = new Random();
    double curCost = oriCost;
    double newCost = 0;
    for (int i = 0;
        i < maxIteration && System.currentTimeMillis() - startTime < maxTime;
        i++, temperature *= (1 - coolingRate)) {
      int operation = random.nextInt(2);
      if (operation == 0) {
        swapMeasurementPos();
      } else {
        changeChunkSize();
      }
      // average chunk size is chunk size in disk
      newCost = estimator.estimate(records, measurementOrder, averageChunkSize);
      double probability = Math.abs(random.nextDouble()) % 1.0;
      if (newCost < curCost || Math.exp((curCost - newCost) / temperature) > probability) {
        curCost = newCost;
      } else {
        undo();
      }
      if (config.isVerbose() && i % config.getLogEpoch() == 0) {
        logger.info(
            "{} rounds have been optimized, {} rounds in total. Current cost: {}, origin cost: {}",
            i,
            config.getSAMaxIteration(),
            curCost,
            oriCost);
      }
    }
    return new Pair<>(measurementOrder, averageChunkSize);
  }

  private void getChunkBound() {
    for (QueryRecord record : records) {
      try {
        Pair<Long, Long> chunkBoundForSingleRecord = getChunkBound(record);
        chunkLowerBound = Math.min(chunkLowerBound, chunkBoundForSingleRecord.left);
        chunkUpperBound = Math.max(chunkUpperBound, chunkBoundForSingleRecord.right);
      } catch (StorageGroupNotSetException | DataSizeInfoNotExistsException e) {
        continue;
      }
    }
    double compressionRatio = CompressionRatio.getInstance().getRatio();
    chunkLowerBound = (long) (chunkLowerBound * compressionRatio);
    chunkUpperBound = (long) (chunkUpperBound * compressionRatio);
    // get the average memory space for each measurement
    try {
      IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
      long memTableThreshold = ioTDBConfig.getMemtableSizeThreshold();
      int measurementNum =
          getMeasurementCountOfStorageGroup(MManager.getInstance().getStorageGroupPath(device));
      long averageSizeForSingleChunkInMem = memTableThreshold / measurementNum;
      // chunk upper bound should not be bigger than the average size of the each measurement
      chunkUpperBound = Math.min(averageSizeForSingleChunkInMem, chunkUpperBound);
    } catch (StorageGroupNotSetException e) {
      e.printStackTrace();
    }
  }

  private Pair<Long, Long> getChunkBound(QueryRecord queryRecord)
      throws StorageGroupNotSetException, DataSizeInfoNotExistsException {
    List<String> measurements = queryRecord.getMeasurements();
    PartialPath device = queryRecord.getDevice();
    long span = queryRecord.getSpan();
    SampleRateKeeper keeper = SampleRateKeeper.getInstance();
    BigDecimal totalSampleRate = BigDecimal.valueOf(0);
    int totalNum = 0;
    if (!keeper.hasSampleRateForDevice(queryRecord.getDevice().getFullPath())) {
      try {
        keeper.updateSampleRate(queryRecord.getDevice().getFullPath());
      } catch (Exception e) {
      }
    }
    for (String measurement : measurements) {
      try {
        totalSampleRate =
            totalSampleRate.add(
                BigDecimal.valueOf(keeper.getSampleRate(device.getFullPath(), measurement)));
        totalNum++;
      } catch (SampleRateNoExistsException e) {
        continue;
      }
    }
    double averageSampleRate = totalSampleRate.divide(BigDecimal.valueOf(totalNum)).doubleValue();
    MManager manager = MManager.getInstance();
    long visitChunkSize =
        DataSizeEstimator.getInstance()
            .getChunkSizeInDisk(
                manager.getStorageGroupPath(device).getFullPath(),
                (long) (averageSampleRate * span));
    long lowerBound = (long) (visitChunkSize * lowerBoundRatio);
    long upperBound = (long) (visitChunkSize * upperBoundRatio);
    return new Pair<>(lowerBound, upperBound);
  }

  private void swapMeasurementPos() {
    Random random = new Random();
    int left = random.nextInt(measurementOrder.size());
    int right = random.nextInt(measurementOrder.size());
    while (left == right) {
      left = random.nextInt(measurementOrder.size());
      right = random.nextInt(measurementOrder.size());
    }
    String tmp = measurementOrder.get(left);
    measurementOrder.set(left, measurementOrder.get(right));
    measurementOrder.set(right, tmp);
    preSwapLeft = left;
    preSwapRight = right;
    preOperation = 0;
  }

  private void changeChunkSize() {
    long newChunkSize =
        Math.abs(new Random().nextLong()) % (chunkUpperBound - chunkLowerBound) + chunkLowerBound;
    preChunkSize = averageChunkSize;
    averageChunkSize = newChunkSize;
    preOperation = 1;
  }

  private void undo() {
    if (preOperation == 0) {
      String tmp = measurementOrder.get(preSwapLeft);
      measurementOrder.set(preSwapLeft, measurementOrder.get(preSwapRight));
      measurementOrder.set(preSwapRight, tmp);
    } else {
      averageChunkSize = preChunkSize;
    }
  }

  private int getMeasurementCountOfStorageGroup(PartialPath storageGroup) {
    MManager manager = MManager.getInstance();
    try {
      List<PartialPath> partialPaths = manager.getAllTimeseriesPath(storageGroup);
      return partialPaths.size();
    } catch (MetadataException e) {
      return -1;
    }
  }
}
