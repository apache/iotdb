package org.apache.iotdb.db.layoutoptimize.layoutoptimizer.optimizerimpl;

import org.apache.iotdb.db.exception.layoutoptimize.DataSizeInfoNotExistsException;
import org.apache.iotdb.db.exception.layoutoptimize.SampleRateNoExistsException;
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
  private long chunkUpperBound = Long.MIN_VALUE;;
  private long chunkLowerBound = Long.MAX_VALUE;
  private double lowerBoundRatio = 0.5;
  private double upperBoundRatio = 2.0;
  private int preSwapLeft;
  private int preSwapRight;
  private long preChunkSize;
  private int preOperation;
  private static final Logger logger = LoggerFactory.getLogger(TCAOptimizer.class);

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
    long blockInterval = chunkUpperBound - chunkLowerBound;
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
      newCost = estimator.estimate(records, measurementOrder, averageChunkSize);
      double probability = Math.abs(random.nextDouble()) % 1.0;
      if (newCost < curCost || Math.exp((curCost - newCost) / temperature) > probability) {
        curCost = newCost;
      } else {
        undo();
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
  }

  private Pair<Long, Long> getChunkBound(QueryRecord queryRecord)
      throws StorageGroupNotSetException, DataSizeInfoNotExistsException {
    List<String> measurements = queryRecord.getMeasurements();
    PartialPath device = queryRecord.getDevice();
    long span = queryRecord.getSpan();
    SampleRateKeeper keeper = SampleRateKeeper.getInstance();
    BigDecimal totalSampleRate = BigDecimal.valueOf(0);
    int totalNum = 0;
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
            .getChunkSizeInMemory(
                manager.getStorageGroupPath(device).getFullPath(),
                (long) (averageSampleRate * span));
    long lowerBound = (long) (visitChunkSize * lowerBoundRatio);
    long upperBound = (long) (visitChunkSize * upperBoundRatio);
    // TODO: get the min value of upper bound and memory limit val
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
}
