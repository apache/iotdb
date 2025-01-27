package org.apache.iotdb.session.util.retry;

public interface RetryDecision {
    boolean shouldRetry();

    long getSleepTimeIfRetry();


}
