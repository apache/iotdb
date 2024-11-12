package org.apache.iotdb.db.queryengine.processor.process;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.db.queryengine.processor.WorkProcessor;
import org.apache.iotdb.db.queryengine.processor.state.ProcessState;

import java.util.function.Supplier;

import static org.apache.iotdb.db.queryengine.processor.WorkProcessorUtils.getNextState;

public class BlockingProcessor<T> implements Processor<T> {
  final WorkProcessor<T> processor;
  final Supplier<ListenableFuture<Void>> futureSupplier;
  ProcessState<T> state;

  public BlockingProcessor(
      WorkProcessor<T> processor, Supplier<ListenableFuture<Void>> futureSupplier) {
    this.processor = processor;
    this.futureSupplier = futureSupplier;
  }

  @Override
  public ProcessState<T> process() {
    if (state == null) {
      state = getNextState(processor);
    }

    ListenableFuture<Void> future = futureSupplier.get();
    if (!future.isDone()) {
      if (state.getType() == ProcessState.Type.YIELD) {
        // clear yielded state to continue computations in the next iteration
        state = null;
      }
      return ProcessState.blocked(future);
    }

    ProcessState<T> result = state;
    state = null;
    return result;
  }
}
