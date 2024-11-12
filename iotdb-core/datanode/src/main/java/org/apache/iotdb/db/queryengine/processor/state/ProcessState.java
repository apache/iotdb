package org.apache.iotdb.db.queryengine.processor.state;

import com.google.common.util.concurrent.ListenableFuture;

public class ProcessState<T> {
  private static final ProcessState<?> YIELD_STATE = new ProcessState<>(Type.YIELD, null, null);
  private static final ProcessState<?> FINISHED_STATE =
      new ProcessState<>(Type.FINISHED, null, null);

  public enum Type {
    BLOCKED,
    YIELD,
    RESULT,
    FINISHED
  }

  private final Type type;
  private final T result;
  private final ListenableFuture<Void> blocked;

  private ProcessState(Type type, T result, ListenableFuture<Void> blocked) {
    this.type = type;
    this.result = result;
    this.blocked = blocked;
  }

  public static <T> ProcessState<T> blocked(ListenableFuture<Void> blocked) {
    return new ProcessState<>(Type.BLOCKED, null, blocked);
  }

  @SuppressWarnings("unchecked")
  public static <T> ProcessState<T> yielded() {
    return (ProcessState<T>) YIELD_STATE;
  }

  public static <T> ProcessState<T> ofResult(T result) {
    return new ProcessState<>(Type.RESULT, result, null);
  }

  @SuppressWarnings("unchecked")
  public static <T> ProcessState<T> finished() {
    return (ProcessState<T>) FINISHED_STATE;
  }

  public Type getType() {
    return type;
  }

  public T getResult() {
    return result;
  }

  public ListenableFuture<Void> getBlocked() {
    return blocked;
  }
}
