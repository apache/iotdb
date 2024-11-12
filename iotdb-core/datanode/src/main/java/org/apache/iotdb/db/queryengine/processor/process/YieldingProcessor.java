package org.apache.iotdb.db.queryengine.processor.process;

import org.apache.iotdb.db.queryengine.processor.WorkProcessor;
import org.apache.iotdb.db.queryengine.processor.state.ProcessState;

import java.util.function.BooleanSupplier;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.processor.WorkProcessorUtils.getNextState;

public class YieldingProcessor<T> implements Processor<T> {
  final WorkProcessor<T> processor;
  final BooleanSupplier yieldSignal;
  boolean lastProcessYielded;

  public YieldingProcessor(WorkProcessor<T> processor, BooleanSupplier yieldSignal) {
    this.processor = requireNonNull(processor, "processor is null");
    this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
  }

  @Override
  public ProcessState<T> process() {
    if (!lastProcessYielded && yieldSignal.getAsBoolean()) {
      lastProcessYielded = true;
      return ProcessState.yielded();
    }
    lastProcessYielded = false;

    return getNextState(processor);
  }
}
