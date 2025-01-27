package org.apache.iotdb.commons.utils.retry;

public interface RetryDecision {
    boolean shouldRetry();

    long getSleepTimeIfRetry();
}
