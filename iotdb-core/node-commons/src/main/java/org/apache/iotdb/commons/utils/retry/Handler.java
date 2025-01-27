package org.apache.iotdb.commons.utils.retry;

import java.util.Optional;

public interface Handler<T> {
  RetryDecision onExecutionResult(T result);
  RetryDecision onException(Exception exception);
  T onFinalResult(T lastResult, Exception lastException) throws Exception;
}
