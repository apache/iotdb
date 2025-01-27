package org.apache.iotdb.session.util.retry;

import java.util.Optional;

public class TaskResult <T, E extends Throwable> {
  private final T result;
  private final E exception;

  private TaskResult(T result, E exception) {
    this.result = result;
    this.exception = exception;
  }
  static <T> TaskResult<T, Throwable> from(T result)  {
    return new TaskResult<>(result, null);
  }

  static <E extends Throwable> TaskResult<Object, E> fromException(E exception) {
    return new TaskResult<>(null, exception);
  }

  public boolean isPresent() {
    return Optional.ofNullable(result).isPresent();
  }

  public Optional<T> getResult() {
    return Optional.ofNullable(result);
  }

  public Optional<E> getException() {
    return Optional.ofNullable(exception);
  }
}
