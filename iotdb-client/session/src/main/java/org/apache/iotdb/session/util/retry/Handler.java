package org.apache.iotdb.session.util.retry;

public interface Handler<T> {
  RetryDecision onExecutionResult(T result);
  RetryDecision onException(Exception exception);
  T onFinalResult(T lastResult, Exception lastException) throws Exception;
}
