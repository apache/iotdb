/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.DeviceEntrySpillNotFoundException;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Behavioural lifecycle tests replacing the obsolete queryLocks reflection audit. */
public class DeviceEntryCleanupBoundaryAuditTest {
  private Path temporaryDirectory;
  private String originalSortTmpDir;
  private DeviceEntrySpillManager manager;

  @Before
  public void setUp() throws Exception {
    temporaryDirectory = Files.createTempDirectory("device-entry-aug31-cleanup-");
    originalSortTmpDir = IoTDBDescriptor.getInstance().getConfig().getSortTmpDir();
    IoTDBDescriptor.getInstance().getConfig().setSortTmpDir(temporaryDirectory.toString());
    manager = DeviceEntrySpillManager.getInstance();
  }

  @After
  public void tearDown() throws Exception {
    try {
      manager.clearStaleData();
      FileUtils.deleteDirectory(temporaryDirectory.toFile());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSortTmpDir(originalSortTmpDir);
    }
  }

  @Test
  public void duplicateAndUnknownFinishShouldNotGrowRegistry() throws Exception {
    int before = registry().size();
    for (int i = 0; i < 1000; i++) {
      manager.finishSegmentDataSet("unknown-query-" + i, "scan");
      manager.finishSegmentDataSet("unknown-query-" + i, "scan");
    }
    assertEquals(before, registry().size());
    assertFalse(
        java.util.Arrays.stream(DeviceEntrySpillManager.class.getDeclaredFields())
            .anyMatch(field -> field.getName().equals("queryLocks")));
    System.out.printf(
        "REGISTRY_LEAK_AUDIT requests=2000 before=%d after=%d queryLocksField=false%n",
        before, registry().size());
  }

  @Test
  public void segmentReleaseAfterQueryCleanupShouldBeIdempotent() throws Exception {
    Path owner = manager.register("q-release", new PlanNodeId("scan"));
    segment(owner);
    manager.deregisterQuery("q-release");
    for (int i = 0; i < 20; i++) {
      manager.deleteSegment("q-release", "scan", 0);
      manager.finishSegmentDataSet("q-release", "scan");
    }
    assertFalse(Files.exists(owner.getParent()));
    System.out.println("RELEASE_AFTER_CLEANUP rounds=20 exceptions=0 files=0");
  }

  @Test
  public void queryCleanupAndSegmentFinishCanRunConcurrently() throws Exception {
    for (int round = 0; round < 100; round++) {
      String queryId = "q-concurrent-cleanup-" + round;
      PlanNodeId planNodeId = new PlanNodeId("scan");
      Path owner = manager.register(queryId, planNodeId);
      segment(owner);

      CountDownLatch start = new CountDownLatch(1);
      ExecutorService executor = Executors.newFixedThreadPool(2);
      Future<?> queryCleanup =
          executor.submit(
              () -> {
                start.await();
                manager.deregisterQuery(queryId);
                return null;
              });
      Future<?> segmentFinish =
          executor.submit(
              () -> {
                start.await();
                manager.finishSegmentDataSet(queryId, planNodeId.getId());
                return null;
              });
      start.countDown();
      queryCleanup.get(5, TimeUnit.SECONDS);
      segmentFinish.get(5, TimeUnit.SECONDS);
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertFalse(Files.exists(owner.getParent()));
      assertFalse(registry().containsKey(queryId));
    }
    System.out.println(
        "CONCURRENT_QUERY_CLEANUP_AND_SEGMENT_FINISH rounds=100 exceptions=0 files=0");
  }

  @Test
  public void finishingLastOldOwnerMustNotDeleteConcurrentlyRegisteredNewOwner() throws Exception {
    runLastOwnerRace(false);
  }

  @Test
  public void finishingOldOwnerMustNotDeleteReplacementQueryRegistration() throws Exception {
    runLastOwnerRace(true);
  }

  private void runLastOwnerRace(boolean replaceRegistration) throws Exception {
    String queryId = replaceRegistration ? "q-replacement-generation" : "q-same-generation";
    Path oldOwner = manager.register(queryId, new PlanNodeId("old-owner"));
    segment(oldOwner);
    Set<Path> originalOwners = registry().get(queryId);
    SnapshotEmptySet controlledOwners = new SnapshotEmptySet(originalOwners);
    registry().put(queryId, controlledOwners);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> finishing =
        executor.submit(
            () -> {
              manager.deregisterOwner(queryId, oldOwner);
              return null;
            });
    Path newSegment;
    try {
      assertTrue(
          "old cleanup must reach the empty-snapshot scheduling point",
          controlledOwners.emptyObserved.await(5, TimeUnit.SECONDS));
      if (replaceRegistration) {
        manager.deregisterQuery(queryId);
      }
      Path newOwner = manager.register(queryId, new PlanNodeId("new-owner"));
      newSegment = segment(newOwner);
      assertTrue(Files.isRegularFile(newSegment));
      assertTrue(registry().get(queryId).contains(newOwner));
      controlledOwners.continueCleanup.countDown();
      finishing.get(5, TimeUnit.SECONDS);
      boolean newSegmentExists = Files.isRegularFile(newSegment);
      boolean newOwnerRegistered =
          registry().containsKey(queryId) && registry().get(queryId).contains(newOwner);
      System.out.printf(
          "OWNER_LIFECYCLE_RACE replacementSet=%s newSegmentExists=%s newOwnerRegistered=%s path=%s%n",
          replaceRegistration, newSegmentExists, newOwnerRegistered, newSegment);
      assertTrue(
          "finishing the previous owner must not delete an independently registered live owner",
          newSegmentExists);
      assertTrue("live owner must remain registered", newOwnerRegistered);
      assertEquals(1, manager.readSegment(queryId, "new-owner", 0).length);
    } finally {
      controlledOwners.continueCleanup.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void finishMustNotDeleteAnUnregisteredAncestor() throws Exception {
    String queryId = "q-exact-owner";
    Path first = manager.register(queryId, new PlanNodeId("scan-a"));
    Path second = manager.register(queryId, new PlanNodeId("scan-b"));
    segment(first);
    Path protectedSibling = segment(second);
    try {
      manager.finishSegmentDataSet(queryId, "scan-a/..");
    } catch (IllegalArgumentException expectedRejection) {
      // Rejecting a non-owner identity is also an acceptable internal API contract.
    }
    boolean siblingExists = Files.isRegularFile(protectedSibling);
    System.out.printf(
        "INTERNAL_FINISH_BOUNDARY siblingExists=%s; input=scan-a/..; all paths remain inside test temp directory%n",
        siblingExists);
    assertTrue(
        "an unregistered parent path must not delete another registered owner", siblingExists);
  }

  @Test
  public void removedSegmentWarningShouldRenderTheActualPath() throws Exception {
    String queryId = "q-rendered-warning-audit";
    Logger logger = (Logger) LoggerFactory.getLogger(LocalSegmentDeviceEntrySource.class);
    Level originalLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    try (LocalSegmentDeviceEntrySource source =
        new LocalSegmentDeviceEntrySource(
            new DeviceEntryDataSetHandle(
                queryId, new PlanNodeId("scan"), new TEndPoint("127.0.0.1", 1), 1, 1, false))) {
      assertThrows(DeviceEntrySpillNotFoundException.class, source::nextBatch);
      assertFalse(appender.list.isEmpty());
      String warning = appender.list.get(appender.list.size() - 1).getFormattedMessage();
      System.out.printf("REMOVED_SEGMENT_WARNING rendered=%s%n", warning);
      assertTrue(
          "warning should identify the removed query/segment, not leave a %s placeholder",
          warning.contains(queryId));
    } finally {
      logger.detachAppender(appender);
      appender.stop();
      logger.setLevel(originalLevel);
    }
  }

  private static Path segment(Path owner) throws Exception {
    Path directory = owner.resolve("fi");
    Files.createDirectories(directory);
    return Files.write(directory.resolve("segment-000000.bin"), new byte[] {1});
  }

  @SuppressWarnings("unchecked")
  private ConcurrentHashMap<String, Set<Path>> registry() throws Exception {
    Field field = DeviceEntrySpillManager.class.getDeclaredField("queryDirectories");
    field.setAccessible(true);
    return (ConcurrentHashMap<String, Set<Path>>) field.get(manager);
  }

  /** Delegates all mutations unchanged and only freezes the natural isEmpty/check-act gap. */
  private static final class SnapshotEmptySet extends AbstractSet<Path> {
    private final Set<Path> delegate;
    private final AtomicBoolean used = new AtomicBoolean();
    private final CountDownLatch emptyObserved = new CountDownLatch(1);
    private final CountDownLatch continueCleanup = new CountDownLatch(1);

    private SnapshotEmptySet(Set<Path> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Iterator<Path> iterator() {
      return delegate.iterator();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean add(Path path) {
      return delegate.add(path);
    }

    @Override
    public boolean remove(Object path) {
      return delegate.remove(path);
    }

    @Override
    public boolean isEmpty() {
      boolean snapshot = delegate.isEmpty();
      if (snapshot && used.compareAndSet(false, true)) {
        emptyObserved.countDown();
        try {
          if (!continueCleanup.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("test did not release cleanup scheduling barrier");
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new AssertionError(interrupted);
        }
      }
      return snapshot;
    }
  }
}
