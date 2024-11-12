package org.apache.iotdb.db.queryengine.workers;

import org.apache.iotdb.db.queryengine.workers.WorkerProcessorTestUtils.Transform;
import org.apache.iotdb.db.queryengine.workers.state.ProcessState;
import org.apache.iotdb.db.queryengine.workers.state.TransformationState;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.queryengine.workers.WorkerProcessorTestUtils.assertBlock;
import static org.apache.iotdb.db.queryengine.workers.WorkerProcessorTestUtils.assertFinish;
import static org.apache.iotdb.db.queryengine.workers.WorkerProcessorTestUtils.assertResult;
import static org.apache.iotdb.db.queryengine.workers.WorkerProcessorTestUtils.assertUnblock;
import static org.apache.iotdb.db.queryengine.workers.WorkerProcessorTestUtils.assertYield;
import static org.apache.iotdb.db.queryengine.workers.WorkerProcessorTestUtils.processorFrom;
import static org.apache.iotdb.db.queryengine.workers.WorkerProcessorTestUtils.transformationFrom;

public class WorkerProcessorTest {
  @Test
  public void testYield() {
    SettableFuture<Void> future = SettableFuture.create();

    List<ProcessState<Integer>> baseScenario =
        ImmutableList.of(
            ProcessState.ofResult(1),
            ProcessState.blocked(future),
            ProcessState.ofResult(2),
            ProcessState.finished());

    AtomicBoolean yieldSignal = new AtomicBoolean();
    WorkProcessor<Integer> processor = processorFrom(baseScenario).yielding(yieldSignal::get);

    assertResult(processor, 1);

    // Set yield
    yieldSignal.set(true);
    assertYield(processor);

    // Block and still yield
    assertBlock(processor);
    assertUnblock(processor, future);
    assertYield(processor);

    yieldSignal.set(false);
    // No yield
    assertResult(processor, 2);
    assertFinish(processor);
  }

  @Test
  public void testBlock() {
    SettableFuture<Void> phase1 = SettableFuture.create();

    List<ProcessState<Integer>> scenario =
        ImmutableList.of(
            ProcessState.blocked(phase1),
            ProcessState.yielded(),
            ProcessState.ofResult(1),
            ProcessState.finished());

    AtomicReference<SettableFuture<Void>> phase2 = new AtomicReference<>(SettableFuture.create());
    WorkProcessor<Integer> processor = processorFrom(scenario).blocking(phase2::get);

    // phase2 blocking future overrides phase1
    assertBlock(processor);
    assertUnblock(processor, phase2.get());
    assertBlock(processor);
    assertUnblock(processor, phase1);

    // blocking overrides yielding
    phase2.set(SettableFuture.create());
    assertBlock(processor);
    assertUnblock(processor, phase2.get());
    assertResult(processor, 1);

    // blocking overrides finishing
    phase2.set(SettableFuture.create());
    assertBlock(processor);
    assertUnblock(processor, phase2.get());
    assertFinish(processor);
  }

  @Test
  public void testFinished() {
    AtomicBoolean finished = new AtomicBoolean();
    SettableFuture<Void> future = SettableFuture.create();

    List<ProcessState<Integer>> scenario =
        ImmutableList.of(
            ProcessState.ofResult(1),
            ProcessState.yielded(),
            ProcessState.blocked(future),
            ProcessState.ofResult(2));

    WorkProcessor<Integer> processor = processorFrom(scenario).finishWhen(finished::get);

    assertResult(processor, 1);
    assertYield(processor);
    assertBlock(processor);

    // Blocking over finish
    finished.set(true);
    assertBlock(processor);

    // Finish immediately
    assertUnblock(processor, future);
    assertFinish(processor);
  }

  @Test
  public void testMap() {
    List<ProcessState<Integer>> baseScenario =
        ImmutableList.of(
            ProcessState.ofResult(1), ProcessState.ofResult(2), ProcessState.finished());

    WorkProcessor<Double> processor = processorFrom(baseScenario).map(element -> 2. * element);

    // Like .map() in stream API
    assertResult(processor, 2.);
    assertResult(processor, 4.);
    assertFinish(processor);
  }

  @Test
  public void testFlatMap() {
    List<ProcessState<Integer>> baseScenario =
        ImmutableList.of(
            ProcessState.ofResult(1), ProcessState.ofResult(2), ProcessState.finished());

    WorkProcessor<Double> processor =
        processorFrom(baseScenario)
            .flatMap(
                element ->
                    WorkProcessor.fromIterable(
                        ImmutableList.of((Double) 2. * element, (Double) 3. * element)));

    // Map to List<Integer> and then flatten to Integers
    assertResult(processor, 2.);
    assertResult(processor, 3.);
    assertResult(processor, 4.);
    assertResult(processor, 6.);
    assertFinish(processor);
  }

  @Test
  public void testTransform() {
    SettableFuture<Void> baseFuture = SettableFuture.create();
    List<ProcessState<Integer>> baseScenario =
        ImmutableList.of(
            ProcessState.ofResult(1),
            ProcessState.yielded(),
            ProcessState.blocked(baseFuture),
            ProcessState.ofResult(2),
            ProcessState.ofResult(3),
            ProcessState.finished());

    SettableFuture<Void> future = SettableFuture.create();
    List<Transform<Integer, String>> transformation =
        ImmutableList.of(
            Transform.of(Optional.of(1), TransformationState.needsMoreData()),
            Transform.of(Optional.of(2), TransformationState.ofResult("foo")),
            Transform.of(Optional.of(3), TransformationState.blocked(future)),
            Transform.of(Optional.of(3), TransformationState.yielded()),
            Transform.of(Optional.of(3), TransformationState.ofResult("bar", false)),
            Transform.of(Optional.of(3), TransformationState.ofResult("baz", true)),
            Transform.of(Optional.empty(), TransformationState.ofResult("car", false)),
            Transform.of(Optional.empty(), TransformationState.finished()));

    WorkProcessor<String> processor =
        processorFrom(baseScenario).transform(transformationFrom(transformation));

    // Yield since we need more data
    assertYield(processor);

    // Block and unblock from base
    assertBlock(processor);
    assertUnblock(processor, baseFuture);

    // Transform 2 -> "foo"
    assertResult(processor, "foo");

    // Block and unblock in transformation state
    assertBlock(processor);
    assertUnblock(processor, future);

    // Yield by transform
    assertYield(processor);

    // Transform 3 -> "bar"
    assertResult(processor, "bar");

    // Transform 3 -> "baz"
    assertResult(processor, "baz");

    // Transform empty -> "car"
    assertResult(processor, "car");

    // Finish
    assertFinish(processor);
  }

  @Test
  public void testFlatTransform() {
    SettableFuture<Void> baseFuture = SettableFuture.create();
    List<ProcessState<Double>> baseScenario =
        ImmutableList.of(
            ProcessState.ofResult(1.0),
            ProcessState.blocked(baseFuture),
            ProcessState.ofResult(2.0),
            ProcessState.yielded(),
            ProcessState.ofResult(3.0),
            ProcessState.finished());

    SettableFuture<Void> mappedFuture = SettableFuture.create();
    List<ProcessState<Integer>> mapped1 =
        ImmutableList.of(
            ProcessState.ofResult(1),
            ProcessState.yielded(),
            ProcessState.blocked(mappedFuture),
            ProcessState.ofResult(2),
            ProcessState.finished());

    List<ProcessState<Integer>> mapped2 =
        ImmutableList.of(ProcessState.ofResult(3), ProcessState.finished());

    List<Transform<Double, WorkProcessor<Integer>>> transformationScenario =
        ImmutableList.of(
            Transform.of(Optional.of(1.0), TransformationState.ofResult(processorFrom(mapped1))),
            Transform.of(Optional.of(2.0), TransformationState.ofResult(processorFrom(mapped2))),
            Transform.of(Optional.of(3.0), TransformationState.finished()));

    WorkProcessor<Integer> processor =
        processorFrom(baseScenario).flatTransform(transformationFrom(transformationScenario));

    // 1 -> 1 in mapped1
    assertResult(processor, 1);

    // 1 -> yield in mapped1
    assertYield(processor);

    // 1 -> block in mapped1
    assertBlock(processor);
    assertUnblock(processor, mappedFuture);

    // 1 -> 2 in mapped1
    assertResult(processor, 2);
    // mapped1 finish

    // Block and unblock in base
    assertBlock(processor);
    assertUnblock(processor, baseFuture);

    // 2 -> 3 in mapped2
    assertResult(processor, 3);

    // Yield in base
    assertYield(processor);

    // All finishes
    assertFinish(processor);
  }
}
