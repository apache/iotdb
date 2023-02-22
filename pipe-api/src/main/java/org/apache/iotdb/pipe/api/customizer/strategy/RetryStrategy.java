package org.apache.iotdb.pipe.api.customizer.strategy;

public interface RetryStrategy {
  enum RetryStrategyType {

    /** @see SimpleRetryStrategy */
    SIMPLE,

    /** @see ExponentialBackOffStrategy */
    EXPONENTIAL_BACK_OFF
  }

  /**
   * Used by the system to check the retry strategy.
   *
   * @throws RuntimeException if invalid strategy is set
   */
  void check();

  /**
   * Returns the actual retry strategy type.
   *
   * @return the actual retry strategy type
   */
  RetryStrategyType getRetryStrategyType();
}
