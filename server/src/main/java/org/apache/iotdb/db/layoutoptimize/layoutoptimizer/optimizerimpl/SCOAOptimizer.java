package org.apache.iotdb.db.layoutoptimize.layoutoptimizer.optimizerimpl;

import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.layoutoptimize.estimator.CostEstimator;
import org.apache.iotdb.db.layoutoptimize.layoutoptimizer.LayoutOptimizer;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class SCOAOptimizer extends LayoutOptimizer {
  private static final Logger logger = LoggerFactory.getLogger(SCOAOptimizer.class);

  public SCOAOptimizer(PartialPath device) {
    super(device);
  }

  @Override
  public Pair<List<String>, Long> optimize() {
    CostEstimator estimator = CostEstimator.getInstance();
    double oriCost = estimator.estimate(records, measurementOrder, averageChunkSize);
    logger.info("SCOA layout optimizer start with {}", config);
    long startTime = System.currentTimeMillis();
    Random random = new Random();
    double curCost = oriCost;
    double newCost = -1;
    double temperature = config.getSAInitTemperature();
    double coolingRate = config.getSACoolingRate();
    double compressionRatio = CompressionRatio.getInstance().getRatio();
    for (int i = 0;
        i < config.getSAMaxIteration()
            && System.currentTimeMillis() - startTime < config.getSAMaxTime();
        i++, temperature *= (1 - coolingRate)) {
      int left = -1;
      int right = -1;
      do {
        left = random.nextInt(measurementOrder.size());
        right = random.nextInt(measurementOrder.size());
      } while (left == right);
      swapMeasurementPos(left, right);
      // average chunk size is chunk size in disk
      newCost = estimator.estimate(records, measurementOrder, averageChunkSize);
      double probability = Math.abs(random.nextDouble()) % 1.0;
      if (newCost < curCost || Math.exp((curCost - newCost) / temperature) > probability) {
        curCost = newCost;
      } else {
        swapMeasurementPos(left, right);
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
    logger.info(
        "optimization finish, origin cost is {} ms, optimized cost is {} ms", oriCost, curCost);
    return new Pair<>(measurementOrder, averageChunkSize);
  }

  private void swapMeasurementPos(int left, int right) {
    String tmp = measurementOrder.get(left);
    measurementOrder.set(left, measurementOrder.get(right));
    measurementOrder.set(right, tmp);
  }
}
