package org.apache.iotdb.pipe.api.customizer.strategy;

public class ExponentialBackOffStrategy implements RetryStrategy {

  private final int maxRetryTimes;
  private final long initInterval;
  private final double backOffFactor;

  /**
   * @param maxRetryTimes maxRetryTimes > 0
   * @param initInterval retryInterval > 0
   * @param backOffFactor backOffFactor > 0
   */
  public ExponentialBackOffStrategy(int maxRetryTimes, long initInterval, double backOffFactor) {
    this.maxRetryTimes = maxRetryTimes;
    this.initInterval = initInterval;
    this.backOffFactor = backOffFactor;
  }

  @Override
  public void check() {
    if (maxRetryTimes <= 0) {
      throw new RuntimeException(
          String.format("Parameter maxRetryTimes(%d) should be greater than zero.", maxRetryTimes));
    }
    if (initInterval <= 0) {
      throw new RuntimeException(
          String.format("Parameter retryInterval(%d) should be greater than zero.", initInterval));
    }
    if (backOffFactor <= 0) {
      throw new RuntimeException(
          String.format("Parameter backOffFactor(%d) should be greater than zero.", backOffFactor));
    }
  }

  @Override
  public RetryStrategyType getRetryStrategyType() {
    return RetryStrategyType.EXPONENTIAL_BACK_OFF;
  }
}
