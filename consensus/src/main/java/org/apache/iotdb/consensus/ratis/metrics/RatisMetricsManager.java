package org.apache.iotdb.consensus.ratis.metrics;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.concurrent.TimeUnit;

public class RatisMetricsManager {
  private final MetricService metricService = MetricService.getInstance();

  /** Record the time cost in check write condition stage. */
  public void recordWriteCheckCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.RATIS_CONSENSUS_WRITE.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.WRITE_CHECK);
  }

  /** Record the time cost in check read condition stage. */
  public void recordReadCheckCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.RATIS_CONSENSUS_READ.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.READ_CHECK);
  }

  /** Record the time cost in write locally stage. */
  public void recordWriteLocallyCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.RATIS_CONSENSUS_WRITE.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.WRITE_LOCALLY);
  }

  /** Record the time cost in write remotely stage. */
  public void recordWriteRemotelyCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.RATIS_CONSENSUS_WRITE.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.WRITE_REMOTELY);
  }

  /** Record the total write time cost. */
  public void recordTotalWriteCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.RATIS_CONSENSUS_WRITE.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.TOTAL_WRITE_TIME);
  }

  /** Record the total read time cost. */
  public void recordTotalReadCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.RATIS_CONSENSUS_READ.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.TOTAL_READ_TIME);
  }

  /** Record the time cost in submit read request stage. */
  public void recordReadRequestCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.RATIS_CONSENSUS_READ.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.SUBMIT_READ_REQUEST);
  }

  /** Record the time cost in write state machine stage. */
  public void recordWriteStateMachineCost(long costTimeInNanos) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.RATIS_CONSENSUS_WRITE.toString(),
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.WRITE_STATE_MACHINE);
  }

  public static RatisMetricsManager getInstance() {
    return RatisMetricsManagerHolder.INSTANCE;
  }

  private static class RatisMetricsManagerHolder {
    private static final RatisMetricsManager INSTANCE = new RatisMetricsManager();

    private RatisMetricsManagerHolder() {
      // empty constructor
    }
  }
}
