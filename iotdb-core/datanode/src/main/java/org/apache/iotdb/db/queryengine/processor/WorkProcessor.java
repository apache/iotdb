package org.apache.iotdb.db.queryengine.processor;

import org.apache.iotdb.db.queryengine.processor.process.Processor;
import org.apache.iotdb.db.queryengine.processor.state.ProcessState;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

public class WorkProcessor<T> {
  Processor<T> process;
  ProcessState<T> state;

  WorkProcessor(Processor<T> processor) {
    this(processor, ProcessState.yielded());
  }

  WorkProcessor(Processor<T> process, ProcessState<T> initialState) {
    this.process = process;
    this.state = initialState;
  }

  static <T> WorkProcessor<T> create(Processor<T> process) {
    return WorkProcessorUtils.create(process);
  }

  static <T> WorkProcessor<T> fromIterable(Iterable<T> iterable) {
    return WorkProcessorUtils.fromIterator(iterable.iterator());
  }

  static <T> WorkProcessor<T> fromIterator(Iterator<T> iterator) {
    return WorkProcessorUtils.fromIterator(iterator);
  }

  public boolean process() {
    if (isBlocked()) {
      return false;
    }
    if (isFinished()) {
      return true;
    }
    state = process.process();

    if (state.getType() == ProcessState.Type.FINISHED) {
      process = null;
      return true;
    }

    return state.getType() == ProcessState.Type.RESULT;
  }

  public boolean isBlocked() {
    return state.getType() == ProcessState.Type.BLOCKED && !state.getBlocked().isDone();
  }

  public ListenableFuture<Void> getBlockedFuture() {
    if (state.getType() != ProcessState.Type.BLOCKED) {
      throw new IllegalStateException("Must be blocked to get blocked future");
    }

    return state.getBlocked();
  }

  public boolean isFinished() {
    return state.getType() == ProcessState.Type.FINISHED;
  }

  public T getResult() {
    if (state.getType() != ProcessState.Type.RESULT) {
      throw new IllegalStateException("process() must return true and must not be finished");
    }

    return state.getResult();
  }

  WorkProcessor<T> yielding(BooleanSupplier yieldSignal) {
    return WorkProcessorUtils.yielding(this, yieldSignal);
  }

  public <R> WorkProcessor<R> transform(Transformer<T, R> transformation) {
    return WorkProcessorUtils.transform(this, transformation);
  }

  public <R> WorkProcessor<R> map(Function<T, R> mapper) {
    return WorkProcessorUtils.map(this, mapper);
  }

  public WorkProcessor<T> blocking(Supplier<ListenableFuture<Void>> futureSupplier) {
    return WorkProcessorUtils.blocking(this, futureSupplier);
  }

  public WorkProcessor<T> finishWhen(BooleanSupplier signal) {
    return WorkProcessorUtils.finishWhen(this, signal);
  }

  public <R> WorkProcessor<R> flatMap(Function<T, WorkProcessor<R>> mapper) {
    return WorkProcessorUtils.flatMap(this, mapper);
  }

  public <R> WorkProcessor<R> flatTransform(Transformer<T, WorkProcessor<R>> transformation) {
    return WorkProcessorUtils.flatTransform(this, transformation);
  }

  public <R> WorkProcessor<R> transformProcessor(
      Function<WorkProcessor<T>, WorkProcessor<R>> transformation) {
    return transformation.apply(this);
  }
}
