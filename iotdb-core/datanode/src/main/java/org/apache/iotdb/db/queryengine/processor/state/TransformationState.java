package org.apache.iotdb.db.queryengine.processor.state;

import com.google.common.util.concurrent.ListenableFuture;

import static java.util.Objects.requireNonNull;

public final class TransformationState<T> {
  private static final TransformationState<?> NEEDS_MORE_DATA_STATE =
      new TransformationState<>(Type.NEEDS_MORE_DATA, true, null, null);
  private static final TransformationState<?> YIELD_STATE =
      new TransformationState<>(Type.YIELD, false, null, null);
  private static final TransformationState<?> FINISHED_STATE =
      new TransformationState<>(Type.FINISHED, false, null, null);

  public enum Type {
    NEEDS_MORE_DATA,
    BLOCKED,
    YIELD,
    RESULT,
    FINISHED
  }

  private final Type type;
  private final boolean needsMoreData;
  private final T result;
  private final ListenableFuture<Void> blocked;

  public TransformationState(
      Type type, boolean needsMoreData, T result, ListenableFuture<Void> blocked) {
    this.type = requireNonNull(type, "type is null");
    this.needsMoreData = needsMoreData;
    this.result = result;
    this.blocked = blocked;
  }

  @SuppressWarnings("unchecked")
  public static <T> TransformationState<T> needsMoreData() {
    return (TransformationState<T>) NEEDS_MORE_DATA_STATE;
  }

  public static <T> TransformationState<T> blocked(ListenableFuture<Void> blocked) {
    return new TransformationState<>(
        Type.BLOCKED, false, null, requireNonNull(blocked, "blocked is null"));
  }

  @SuppressWarnings("unchecked")
  public static <T> TransformationState<T> yielded() {
    return (TransformationState<T>) YIELD_STATE;
  }

  public static <T> TransformationState<T> ofResult(T result) {
    return ofResult(result, true);
  }

  public static <T> TransformationState<T> ofResult(T result, boolean needsMoreData) {
    return new TransformationState<>(
        Type.RESULT, needsMoreData, requireNonNull(result, "result is null"), null);
  }

  @SuppressWarnings("unchecked")
  public static <T> TransformationState<T> finished() {
    return (TransformationState<T>) FINISHED_STATE;
  }

  public Type getType() {
    return type;
  }

  public boolean isNeedsMoreData() {
    return needsMoreData;
  }

  public T getResult() {
    return result;
  }

  public ListenableFuture<Void> getBlocked() {
    return blocked;
  }
}
