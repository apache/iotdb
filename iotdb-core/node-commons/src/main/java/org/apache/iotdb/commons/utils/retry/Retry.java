package org.apache.iotdb.commons.utils.retry;

import java.util.Optional;

public class Retry {
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
