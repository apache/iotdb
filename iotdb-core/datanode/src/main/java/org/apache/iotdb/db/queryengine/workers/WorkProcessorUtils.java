/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.workers;

import org.apache.iotdb.db.queryengine.workers.state.ProcessState;
import org.apache.iotdb.db.queryengine.workers.state.TransformationState;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;

public class WorkProcessorUtils {
  static <T> WorkProcessor<T> create(Processor<T> process) {
    return new WorkProcessor<>(process);
  }

  public static <T> WorkProcessor<T> fromIterator(Iterator<T> iterator) {
    return create(
        () -> {
          if (!iterator.hasNext()) {
            return ProcessState.finished();
          }

          return ProcessState.ofResult(iterator.next());
        });
  }

  /**
   * Receive elements generated from processor and transform them by transformation, finally wrap
   * into a new WorkProcessor.
   *
   * @param processor source processor that generates elements
   * @param transformation transform elements from source processor
   * @return stateful stream, i.e. WorkProcessor
   * @param <T> input elements type
   * @param <R> output elements type
   */
  static <T, R> WorkProcessor<R> transform(
      WorkProcessor<T> processor, Transformer<T, R> transformation) {
    return new WorkProcessor<>(
        new Processor<R>() {
          T element;

          @Override
          public ProcessState<R> process() {
            while (true) {
              if (element == null && !processor.isFinished()) {
                if (processor.process()) {
                  if (!processor.isFinished()) {
                    element = processor.getResult();
                  }
                } else if (processor.isBlocked()) {
                  return ProcessState.blocked(processor.getBlockedFuture());
                } else {
                  return ProcessState.yielded();
                }
              }

              TransformationState<R> state = transformation.process(element);

              if (state.isNeedsMoreData()) {
                checkState(
                    !processor.isFinished(),
                    "Cannot request more data when base processor is finished");
                // set element to empty() in order to fetch a new one
                element = null;
              }

              // pass-through transformation state if it doesn't require new data
              switch (state.getType()) {
                case NEEDS_MORE_DATA:
                  break;
                case BLOCKED:
                  return ProcessState.blocked(state.getBlocked());
                case YIELD:
                  return ProcessState.yielded();
                case RESULT:
                  return ProcessState.ofResult(state.getResult());
                case FINISHED:
                  return ProcessState.finished();
              }
            }
          }
        });
  }

  static <T, R> WorkProcessor<R> map(WorkProcessor<T> processor, Function<T, R> mapper) {
    return processor.transform(
        element -> {
          if (element == null) {
            return TransformationState.finished();
          }

          return TransformationState.ofResult(mapper.apply(element));
        });
  }

  static <T, R> WorkProcessor<R> flatTransform(
      WorkProcessor<T> processor, Transformer<T, WorkProcessor<R>> transformation) {
    return processor.transform(transformation).transformProcessor(WorkProcessorUtils::flatten);
  }

  static <T, R> WorkProcessor<R> flatMap(
      WorkProcessor<T> processor, Function<T, WorkProcessor<R>> mapper) {
    return processor.flatTransform(
        element -> {
          if (element == null) {
            return TransformationState.finished();
          }

          return TransformationState.ofResult(mapper.apply(element));
        });
  }

  static <T> WorkProcessor<T> flatten(WorkProcessor<WorkProcessor<T>> processor) {
    return processor.transform(
        nestedProcessor -> {
          if (nestedProcessor == null) {
            return TransformationState.finished();
          }

          if (nestedProcessor.process()) {
            if (nestedProcessor.isFinished()) {
              return TransformationState.needsMoreData();
            }

            return TransformationState.ofResult(nestedProcessor.getResult(), false);
          }

          if (nestedProcessor.isBlocked()) {
            return TransformationState.blocked(nestedProcessor.getBlockedFuture());
          }

          return TransformationState.yielded();
        });
  }

  static <T> WorkProcessor<T> yielding(WorkProcessor<T> processor, BooleanSupplier yieldSignal) {
    return WorkProcessor.create(new YieldingProcessor<>(processor, yieldSignal));
  }

  static <T> WorkProcessor<T> blocking(
      WorkProcessor<T> processor, Supplier<ListenableFuture<Void>> futureSupplier) {
    return WorkProcessor.create(new BlockingProcessor<>(processor, futureSupplier));
  }

  static <T> WorkProcessor<T> finishWhen(WorkProcessor<T> processor, BooleanSupplier finishSignal) {
    return WorkProcessor.create(
        () -> {
          if (finishSignal.getAsBoolean()) {
            return ProcessState.finished();
          }

          return getNextState(processor);
        });
  }

  public static <T> ProcessState<T> getNextState(WorkProcessor<T> processor) {
    if (processor.process()) {
      if (processor.isFinished()) {
        return ProcessState.finished();
      }

      return ProcessState.ofResult(processor.getResult());
    }

    if (processor.isBlocked()) {
      return ProcessState.blocked(processor.getBlockedFuture());
    }

    return ProcessState.yielded();
  }

  private static class BlockingProcessor<T> implements Processor<T> {
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

  private static class YieldingProcessor<T> implements Processor<T> {
    final WorkProcessor<T> processor;
    final BooleanSupplier yieldSignal;
    boolean lastProcessYielded;

    public YieldingProcessor(WorkProcessor<T> processor, BooleanSupplier yieldSignal) {
      this.processor = processor;
      this.yieldSignal = yieldSignal;
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
}
