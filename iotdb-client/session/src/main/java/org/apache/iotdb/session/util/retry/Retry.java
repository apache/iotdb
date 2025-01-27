package org.apache.iotdb.session.util.retry;

import org.apache.tsfile.utils.TimeDuration;

public class Retry {
  final static public RetryDecision NO_RETRY = new RetryDecision() {
    @Override
    public boolean shouldRetry() {
      return false;
    }

    @Override
    public long getSleepTimeIfRetry() {
      return 0;
    }
  };
  public static <T> T execute(RetriableTask<T> task, Handler<T> handler) throws Exception {
    T result = null;
    RetryDecision decision;
    Exception lastException;

    while (true){
      try {
        result = task.execute();
        decision = handler.onExecutionResult(result);
        lastException = null;
      } catch (Exception e) {
        decision = handler.onException(e);
        lastException = e;
      }

      if (!decision.shouldRetry()) {
        return handler.onFinalResult(result, lastException);
      }
      try {
        Thread.sleep(decision.getSleepTimeIfRetry());
      } catch (InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        // TODO (william) what shall we do here?
      }
    }
  }
}
