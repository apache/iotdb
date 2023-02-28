package org.apache.iotdb.pipe.api.customizer.strategy;

public class SimpleRetryStrategy implements RetryStrategy {

  private final int maxRetryTimes;
  private final long retryInterval;

  /**
   * @param maxRetryTimes maxRetryTimes > 0
   * @param retryInterval retryInterval > 0
   */
  public SimpleRetryStrategy(int maxRetryTimes, long retryInterval) {
    this.maxRetryTimes = maxRetryTimes;
    this.retryInterval = retryInterval;
  }

  @Override
  public void check() {
    if (maxRetryTimes <= 0) {
      throw new RuntimeException(
          String.format("Parameter maxRetryTimes(%d) should be greater than zero.", maxRetryTimes));
    }
    if (retryInterval <= 0) {
      throw new RuntimeException(
          String.format("Parameter retryInterval(%d) should be greater than zero.", retryInterval));
    }
  }

  @Override
  public RetryStrategyType getRetryStrategyType() {
    return RetryStrategyType.SIMPLE;
  }
}
