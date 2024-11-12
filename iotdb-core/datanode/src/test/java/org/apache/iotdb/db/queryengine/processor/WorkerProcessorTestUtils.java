package org.apache.iotdb.db.queryengine.processor;

import org.apache.iotdb.db.queryengine.processor.state.ProcessState;
import org.apache.iotdb.db.queryengine.processor.state.TransformationState;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class WorkerProcessorTestUtils {
  public static <T> void assertBlocks(WorkProcessor<T> processor) {
    Assert.assertFalse(processor.process());
    Assert.assertTrue(processor.isBlocked());
    Assert.assertFalse(processor.isFinished());
    Assert.assertFalse(processor.process());
  }

  public static <T, V> void assertUnblocks(WorkProcessor<T> processor, SettableFuture<V> future) {
    future.set(null);
    Assert.assertFalse(processor.isBlocked());
  }

  public static <T> void assertYields(WorkProcessor<T> processor) {
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

  public static <T> void assertFinishes(WorkProcessor<T> processor) {
    Assert.assertTrue(processor.process());
    Assert.assertFalse(processor.isBlocked());
    Assert.assertTrue(processor.isFinished());
    Assert.assertTrue(processor.process());
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

  public static <T> WorkProcessor<T> processorFrom(List<ProcessState<T>> states) {
    Iterator<ProcessState<T>> iterator = states.iterator();
    return WorkProcessorUtils.create(
        () -> {
          Assert.assertTrue(iterator.hasNext());
          return iterator.next();
        });
  }

  public static class Transform<T, R> {
    private final Optional<T> from;
    private final TransformationState<R> to;

    public static <T, R> Transform<T, R> of(Optional<T> from, TransformationState<R> to) {
      return new Transform<>(from, to);
    }

    private Transform(Optional<T> from, TransformationState<R> to) {
      this.from = requireNonNull(from);
      this.to = requireNonNull(to);
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
