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

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static java.lang.String.format;

public class WorkerProcessorTestUtils {
  public static <T> void assertBlock(WorkProcessor<T> processor) {
    Assert.assertFalse(processor.process());
    Assert.assertTrue(processor.isBlocked());
    Assert.assertFalse(processor.isFinished());
    Assert.assertFalse(processor.process());
  }

  public static <T, V> void assertUnblock(WorkProcessor<T> processor, SettableFuture<V> future) {
    future.set(null);
    Assert.assertFalse(processor.isBlocked());
  }

  public static <T> void assertYield(WorkProcessor<T> processor) {
    Assert.assertFalse(processor.process());
    Assert.assertFalse(processor.isBlocked());
    Assert.assertFalse(processor.isFinished());
  }

  public static <T> void assertResult(WorkProcessor<T> processor, T result) {
    validateResult(processor, actualResult -> Assert.assertEquals(processor.getResult(), result));
  }

  public static <T> void validateResult(WorkProcessor<T> processor, Consumer<T> validator) {
    Assert.assertTrue(processor.process());
    Assert.assertFalse(processor.isBlocked());
    Assert.assertFalse(processor.isFinished());
    validator.accept(processor.getResult());
  }

  public static <T> void assertFinish(WorkProcessor<T> processor) {
    Assert.assertTrue(processor.process());
    Assert.assertFalse(processor.isBlocked());
    Assert.assertTrue(processor.isFinished());
    Assert.assertTrue(processor.process());
  }

  public static <T> WorkProcessor<T> processorFrom(List<ProcessState<T>> states) {
    Iterator<ProcessState<T>> iterator = states.iterator();
    return WorkProcessorUtils.create(
        () -> {
          Assert.assertTrue(iterator.hasNext());
          return iterator.next();
        });
  }

  public static <T, R> Transformer<T, R> transformationFrom(List<Transform<T, R>> transformations) {
    return transformationFrom(transformations, Objects::equals);
  }

  public static <T, R> Transformer<T, R> transformationFrom(
      List<Transform<T, R>> transformations, BiPredicate<T, T> equalsPredicate) {
    Iterator<Transform<T, R>> iterator = transformations.iterator();
    return element -> {
      Assert.assertTrue(iterator.hasNext());
      return iterator
          .next()
          .transform(
              Optional.ofNullable(element),
              (left, right) ->
                  left.isPresent() == right.isPresent()
                      && (!left.isPresent() || equalsPredicate.test(left.get(), right.get())));
    };
  }

  public static class Transform<T, R> {
    private final Optional<T> from;
    private final TransformationState<R> to;

    public static <T, R> Transform<T, R> of(Optional<T> from, TransformationState<R> to) {
      return new Transform<>(from, to);
    }

    private Transform(Optional<T> from, TransformationState<R> to) {
      this.from = from;
      this.to = to;
    }

    private TransformationState<R> transform(
        Optional<T> from, BiPredicate<Optional<T>, Optional<T>> equalsPredicate) {
      Assert.assertTrue(
          format("Expected %s to be equal to %s", from, this.from),
          equalsPredicate.test(from, this.from));
      return to;
    }
  }
}
