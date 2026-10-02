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
package org.apache.iotdb.db.storageengine.dataregion.read.control;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FileReaderManagerConcurrencyTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private final FileReaderManager manager = FileReaderManager.getInstance();
  private ExecutorService workers;

  @Before
  public void setUp() throws Exception {
    manager.closeAndRemoveAllOpenedReaders();
    workers = Executors.newCachedThreadPool();
  }

  @After
  public void tearDown() throws Exception {
    workers.shutdownNow();
    assertTrue(workers.awaitTermination(15, TimeUnit.SECONDS));
    manager.closeAndRemoveAllOpenedReaders();
    assertEmpty();
  }

  private TsFileResource file(int id) throws Exception {
    File f = new File(folder.getRoot(), id + "-1-0-0.tsfile");
    try (TsFileIOWriter writer = new TsFileIOWriter(f)) {
      writer.endFile();
    }
    return new TsFileResource(f);
  }

  private TsFileSequenceReader get(TsFileResource file, boolean closed) throws IOException {
    return manager.get(file.getTsFilePath(), file.getTsFileID(), closed);
  }

  private static void done(Future<?> task) throws Exception {
    task.get(10, TimeUnit.SECONDS);
  }

  private static class Gate implements AutoCloseable {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch open = new CountDownLatch(1);
    final AtomicReference<Thread> owner = new AtomicReference<>();

    void block() {
      owner.set(Thread.currentThread());
      entered.countDown();
      boolean interrupted = false;
      try {
        while (true) {
          try {
            if (!open.await(20, TimeUnit.SECONDS)) {
              throw new AssertionError("gate not released");
            }
            return;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    void await() throws Exception {
      assertTrue(entered.await(10, TimeUnit.SECONDS));
    }

    @Override
    public void close() {
      open.countDown();
    }
  }

  // Prove lock contention and its owner, rather than inferring it from a Future timeout.
  private static void blocked(Thread waiter, Thread owner) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(waiter.getId());
      if (info != null
          && info.getThreadState() == Thread.State.BLOCKED
          && info.getLockOwnerId() == owner.getId()) {
        assertNotNull(info.getLockInfo());
        return;
      }
      Thread.sleep(1);
    }
    fail("worker did not block on the expected monitor owner");
  }

  @Test
  public void slowClosedOpenDoesNotBlockOtherFiles() throws Exception {
    slowOpen(true, false);
  }

  @Test
  public void slowUnclosedOpenDoesNotBlockOtherFiles() throws Exception {
    slowOpen(false, false);
  }

  @Test
  public void slowExternalOpenDoesNotBlockOtherFiles() throws Exception {
    slowOpen(true, true);
  }

  private void slowOpen(boolean closed, boolean external) throws Exception {
    TsFileResource a = file(1);
    TsFileResource b = file(2);
    TsFileResource c = file(3);
    assertNotEquals(a.getTsFileID(), b.getTsFileID());
    TsFileSequenceReader warm = get(b, true);
    try (Gate gate = new Gate()) {
      Future<TsFileSequenceReader> opening =
          workers.submit(
              () ->
                  manager.get(
                      a.getTsFilePath(), a.getTsFileID(), closed, n -> gate.block(), external));
      gate.await();
      AtomicReference<Thread> waiter = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);
      Future<TsFileSequenceReader> same =
          workers.submit(
              () -> {
                waiter.set(Thread.currentThread());
                started.countDown();
                return manager.get(a.getTsFilePath(), a.getTsFileID(), closed, null, external);
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      blocked(waiter.get(), gate.owner.get());
      unrelated(b, c, warm);
      gate.close();
      assertSame(opening.get(10, TimeUnit.SECONDS), same.get(10, TimeUnit.SECONDS));
    }
  }

  private void unrelated(TsFileResource b, TsFileResource c, TsFileSequenceReader warm)
      throws Exception {
    done(
        workers.submit(
            () -> {
              assertSame(warm, get(b, true));
              assertNotNull(get(c, true));
              assertNotNull(get(c, false));
              assertNotNull(manager.get(c.getTsFilePath(), c.getTsFileID(), true, null, true));
              manager.increaseFileReaderReference(b, true);
              manager.decreaseFileReaderReference(b, true);
              manager.increaseExternalFileReaderReference(c.getTsFilePath());
              manager.decreaseExternalFileReaderReference(c.getTsFilePath());
              return null;
            }));
  }

  @Test
  public void slowForceCloseDoesNotBlockOtherFiles() throws Exception {
    slowClose(false);
  }

  @Test
  public void slowLastReleaseDoesNotBlockOtherFiles() throws Exception {
    slowClose(true);
  }

  private void slowClose(boolean release) throws Exception {
    TsFileResource a = file(1);
    TsFileResource b = file(2);
    TsFileResource c = file(3);
    TsFileSequenceReader warm = get(b, true);
    TsFileSequenceReader mock = mock(TsFileSequenceReader.class);
    manager.setReaderForTest(a.getTsFileID(), true, mock);
    if (release) {
      manager.increaseFileReaderReference(a, true);
    }
    try (Gate gate = new Gate()) {
      doAnswer(
              invocation -> {
                gate.block();
                return null;
              })
          .when(mock)
          .close();
      Future<?> closing =
          workers.submit(
              () -> {
                if (release) {
                  manager.decreaseFileReaderReference(a, true);
                } else {
                  manager.closeFileAndRemoveReader(a.getTsFileID());
                }
                return null;
              });
      gate.await();
      AtomicReference<Thread> waiter = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);
      Future<?> acquire =
          workers.submit(
              () -> {
                waiter.set(Thread.currentThread());
                started.countDown();
                manager.increaseFileReaderReference(a, true);
                try {
                  assertNotSame(mock, get(a, true));
                } finally {
                  manager.decreaseFileReaderReference(a, true);
                }
                return null;
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      blocked(waiter.get(), gate.owner.get());
      unrelated(b, c, warm);
      gate.close();
      done(closing);
      done(acquire);
      verify(mock, times(1)).close();
      assertFalse(manager.contains(a, true));
    }
  }

  @Test
  public void failedConstructionPreservesReferencesAndRetries() throws Exception {
    TsFileResource a = file(1);
    manager.increaseFileReaderReference(a, true);
    manager.increaseFileReaderReference(a, true);
    try {
      manager.get(a.getTsFilePath() + ".missing", a.getTsFileID(), true);
      fail("missing file opened");
    } catch (IOException expected) {
      assertFalse(manager.contains(a, true));
    }
    TsFileSequenceReader reader = get(a, true);
    manager.decreaseFileReaderReference(a, true);
    assertSame(reader, get(a, true));
    manager.decreaseFileReaderReference(a, true);
    assertEmpty();
  }

  @Test
  public void closeFailureIsNotServedAndCanBeRetried() throws Exception {
    TsFileResource a = file(1);
    TsFileSequenceReader reader = mock(TsFileSequenceReader.class);
    doThrow(new IOException("injected close failure")).doNothing().when(reader).close();
    manager.setReaderForTest(a.getTsFileID(), true, reader);
    manager.increaseFileReaderReference(a, true);
    manager.decreaseFileReaderReference(a, true);
    assertTrue(a.tryWriteLock());
    a.writeUnlock();
    try {
      get(a, true);
      fail("served a reader after a failed close");
    } catch (IOException expected) {
      assertNotNull(expected.getCause());
    }
    manager.closeFileAndRemoveReader(a.getTsFileID());
    verify(reader, times(2)).close();
    assertEmpty();
    assertNotSame(reader, get(a, true));
  }

  @Test
  public void namespacesAndLegacyReleaseFallback() throws Exception {
    TsFileResource a = file(1);
    TsFileSequenceReader closed = get(a, true);
    TsFileSequenceReader unclosed = get(a, false);
    TsFileSequenceReader external =
        manager.get(a.getTsFilePath(), a.getTsFileID(), true, null, true);
    assertNotSame(closed, unclosed);
    assertNotSame(closed, external);
    assertSame(external, manager.get(new String(a.getTsFilePath()), null, false, null, true));
    manager.increaseFileReaderReference(a, true);
    manager.decreaseFileReaderReference(a, false);
    assertFalse(manager.contains(a, true));
    assertSame(unclosed, get(a, false));
    manager.closeFileAndRemoveReader(a.getTsFileID());
    assertSame(external, manager.get(a.getTsFilePath(), null, true, null, true));
  }

  @Test
  public void clearDrainsReleasePinsEvenWhenInterrupted() throws Exception {
    TsFileResource a = file(1);
    TsFileSequenceReader reader = mock(TsFileSequenceReader.class);
    manager.setReaderForTest(a.getTsFileID(), true, reader);
    Object registry = field("internal");
    Method pin =
        FileReaderManager.class.getDeclaredMethod(
            "pin", Map.class, Object.class, boolean.class, boolean.class);
    pin.setAccessible(true);
    Object entry = null;
    try (Gate gate = new Gate()) {
      doAnswer(
              invocation -> {
                gate.block();
                return null;
              })
          .when(reader)
          .close();
      Future<Boolean> clear =
          workers.submit(
              () -> {
                manager.closeAndRemoveAllOpenedReaders();
                return Thread.interrupted();
              });
      gate.await();
      // Hold a release pin beyond the close phase: a deterministic model of a descheduled release.
      entry = pin.invoke(manager, registry, a.getTsFileID(), false, false);
      assertNotNull(entry);
      gate.owner.get().interrupt();
      Future<?> acquire = acquireDuringClear(a, true);
      gate.close();
      awaitClearDrain(gate.owner.get());
      assertFalse(clear.isDone());
      assertFalse(acquire.isDone());
      Method unpin =
          FileReaderManager.class.getDeclaredMethod(
              "unpin", Map.class, Object.class, entry.getClass());
      unpin.setAccessible(true);
      unpin.invoke(manager, registry, a.getTsFileID(), entry);
      entry = null;
      assertTrue(clear.get(10, TimeUnit.SECONDS));
      done(acquire);
    } finally {
      if (entry != null) {
        Method unpin =
            FileReaderManager.class.getDeclaredMethod(
                "unpin", Map.class, Object.class, entry.getClass());
        unpin.setAccessible(true);
        unpin.invoke(manager, registry, a.getTsFileID(), entry);
      }
    }
  }

  private Future<TsFileSequenceReader> acquireDuringClear(TsFileResource resource, boolean closed)
      throws Exception {
    AtomicReference<Thread> thread = new AtomicReference<>();
    CountDownLatch started = new CountDownLatch(1);
    Future<TsFileSequenceReader> acquire =
        workers.submit(
            () -> {
              thread.set(Thread.currentThread());
              started.countDown();
              return get(resource, closed);
            });
    assertTrue(started.await(10, TimeUnit.SECONDS));
    // An unfinished Future alone does not prove that the worker has reached the admission gate.
    awaitRegistryWait(thread.get(), "pin");
    return acquire;
  }

  private void awaitClearDrain(Thread thread) throws Exception {
    awaitRegistryWait(thread, "drainPins");
  }

  private void awaitRegistryWait(Thread thread, String method) throws Exception {
    Object registryLock = field("registryLock");
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId(), 30);
      if (info != null
          && info.getThreadState() == Thread.State.WAITING
          && info.getLockInfo() != null
          && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(registryLock)) {
        for (StackTraceElement frame : info.getStackTrace()) {
          if (frame.getClassName().equals(FileReaderManager.class.getName())
              && frame.getMethodName().equals(method)) {
            return;
          }
        }
      }
      Thread.sleep(1);
    }
    fail("worker did not wait on registryLock in " + method);
  }

  @Test
  public void concurrentClearAndReleaseComplete() throws Exception {
    TsFileResource a = file(1);
    TsFileSequenceReader reader = mock(TsFileSequenceReader.class);
    manager.setReaderForTest(a.getTsFileID(), true, reader);
    manager.increaseFileReaderReference(a, true);
    try (Gate gate = new Gate()) {
      doAnswer(
              invocation -> {
                gate.block();
                return null;
              })
          .when(reader)
          .close();
      Future<?> first =
          workers.submit(
              () -> {
                manager.closeAndRemoveAllOpenedReaders();
                return null;
              });
      gate.await();
      AtomicReference<Thread> releaseThread = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);
      Future<?> release =
          workers.submit(
              () -> {
                releaseThread.set(Thread.currentThread());
                started.countDown();
                manager.decreaseFileReaderReference(a, true);
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      blocked(releaseThread.get(), gate.owner.get());
      AtomicReference<Thread> secondThread = new AtomicReference<>();
      CountDownLatch secondStarted = new CountDownLatch(1);
      Future<?> second =
          workers.submit(
              () -> {
                secondThread.set(Thread.currentThread());
                secondStarted.countDown();
                manager.closeAndRemoveAllOpenedReaders();
                return null;
              });
      assertTrue(secondStarted.await(10, TimeUnit.SECONDS));
      blocked(secondThread.get(), gate.owner.get());
      gate.close();
      done(first);
      done(second);
      done(release);
      verify(reader, times(1)).close();
      assertEmpty();
    }
  }

  @Test
  public void concurrentChurnReclaimsEntriesAndReaders() throws Exception {
    TsFileResource[] files = {file(1), file(2), file(3)};
    List<Future<?>> tasks = new ArrayList<>();
    for (int t = 0; t < 8; t++) {
      tasks.add(
          workers.submit(
              () -> {
                for (int i = 0; i < 500; i++) {
                  TsFileResource f = files[i % files.length];
                  boolean closed = (i & 1) == 0;
                  manager.increaseFileReaderReference(f, closed);
                  try {
                    assertSame(get(f, closed), get(f, closed));
                  } finally {
                    manager.decreaseFileReaderReference(f, closed);
                  }
                  manager.increaseExternalFileReaderReference(f.getTsFilePath());
                  try {
                    assertNotNull(manager.get(f.getTsFilePath(), null, true, null, true));
                  } finally {
                    manager.decreaseExternalFileReaderReference(f.getTsFilePath());
                  }
                }
                return null;
              }));
    }
    for (Future<?> task : tasks) {
      task.get(60, TimeUnit.SECONDS);
    }
    assertEmpty();
  }

  @Test
  public void forceCloseAttemptsBothSlotsAndClearRetriesFailure() throws Exception {
    TsFileResource a = file(1);
    TsFileSequenceReader closed = mock(TsFileSequenceReader.class);
    TsFileSequenceReader unclosed = mock(TsFileSequenceReader.class);
    manager.setReaderForTest(a.getTsFileID(), true, closed);
    manager.setReaderForTest(a.getTsFileID(), false, unclosed);
    doThrow(new IOException("close failed")).doNothing().when(closed).close();
    try {
      manager.closeFileAndRemoveReader(a.getTsFileID());
      fail("close failure lost");
    } catch (IOException expected) {
      verify(unclosed).close();
      assertFalse(manager.contains(a, false));
    }
    manager.closeAndRemoveAllOpenedReaders();
    verify(closed, times(2)).close();
    assertEmpty();
  }

  @Test
  public void pinnedEmptyEntryCannotBeReplaced() throws Exception {
    TsFileResource a = file(1);
    Object registry = field("internal");
    Method pin =
        FileReaderManager.class.getDeclaredMethod(
            "pin", Map.class, Object.class, boolean.class, boolean.class);
    pin.setAccessible(true);
    Object entry = pin.invoke(manager, registry, a.getTsFileID(), true, true);
    try {
      manager.closeFileAndRemoveReader(a.getTsFileID());
      done(workers.submit(() -> get(a, true)));
      assertSame(entry, ((Map<?, ?>) registry).get(a.getTsFileID()));
      manager.closeFileAndRemoveReader(a.getTsFileID());
      assertSame(entry, ((Map<?, ?>) registry).get(a.getTsFileID()));
    } finally {
      Method unpin =
          FileReaderManager.class.getDeclaredMethod(
              "unpin", Map.class, Object.class, entry.getClass());
      unpin.setAccessible(true);
      unpin.invoke(manager, registry, a.getTsFileID(), entry);
    }
    assertEmpty();
  }

  @Test
  public void failedRegistrationReleasesResourceLock() throws Exception {
    TsFileResource resource = mock(TsFileResource.class);
    org.mockito.Mockito.when(resource.getTsFileID())
        .thenThrow(new IllegalStateException("injected"));
    try {
      manager.increaseFileReaderReference(resource, true);
      fail("registration failure lost");
    } catch (IllegalStateException expected) {
      verify(resource).readLock();
      verify(resource).readUnlock();
    }
    assertEmpty();
  }

  @Test
  public void interruptedClearWaitsForExistingOperation() throws Exception {
    TsFileResource a = file(1);
    try (Gate gate = new Gate()) {
      Future<?> open =
          workers.submit(
              () -> manager.get(a.getTsFilePath(), a.getTsFileID(), true, n -> gate.block()));
      gate.await();
      AtomicReference<Thread> clearThread = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);
      Future<Boolean> clear =
          workers.submit(
              () -> {
                clearThread.set(Thread.currentThread());
                started.countDown();
                manager.closeAndRemoveAllOpenedReaders();
                return Thread.interrupted();
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      awaitClearDrain(clearThread.get());
      clearThread.get().interrupt();
      Future<?> acquire = acquireDuringClear(a, false);
      assertFalse(clear.isDone());
      assertFalse(acquire.isDone());
      gate.close();
      done(open);
      assertTrue(clear.get(10, TimeUnit.SECONDS));
      done(acquire);
      assertFalse(manager.contains(a, true));
      assertTrue(manager.contains(a, false));
    }
  }

  private Object field(String name) throws Exception {
    Field field = FileReaderManager.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(manager);
  }

  private void assertEmpty() throws Exception {
    assertEquals(0, field("pins"));
    assertTrue(((Map<?, ?>) field("internal")).isEmpty());
    assertTrue(((Map<?, ?>) field("external")).isEmpty());
    for (String name : new String[] {"closedCount", "unclosedCount", "externalCount"}) {
      assertEquals(0, ((java.util.concurrent.atomic.AtomicInteger) field(name)).get());
    }
  }
}
