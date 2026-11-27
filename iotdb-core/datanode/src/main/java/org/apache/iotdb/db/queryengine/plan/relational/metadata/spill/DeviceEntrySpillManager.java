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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DeviceEntrySpillManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceEntrySpillManager.class);

  private final ConcurrentHashMap<String, Set<Path>> queryDirectories = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<DeviceEntryDiskSpiller>> querySpillers =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<DeviceEntryFileSpillerReader>> queryReaders =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, QueryDirectoryLock> queryDirectoryLocks =
      new ConcurrentHashMap<>();

  private DeviceEntrySpillManager() {}

  public static DeviceEntrySpillManager getInstance() {
    return DeviceEntrySpillManagerHolder.INSTANCE;
  }

  public Path register(String queryId, PlanNodeId planNodeId) throws IOException {
    QueryDirectoryLock lock =
        queryDirectoryLocks.computeIfAbsent(queryId, ignored -> new QueryDirectoryLock());
    if (!lock.tryAcquireReference()) {
      throw new NoSuchFileException(resolveQueryDirectory(queryId).toString());
    }
    return withQueryDirectoryLock(
        queryId,
        lock,
        () -> {
          Path ownerDirectory = resolveOwnerDirectory(queryId, planNodeId.getId());
          Files.createDirectories(ownerDirectory);
          queryDirectories
              .computeIfAbsent(queryId, ignored -> ConcurrentHashMap.newKeySet())
              .add(ownerDirectory);
          return ownerDirectory;
        },
        false,
        false);
  }

  public void deregisterOwner(String queryId, Path ownerDirectory) throws IOException {
    QueryDirectoryLock lock = tryAcquireQueryDirectoryLock(queryId);
    if (lock == null) {
      return;
    }
    withQueryDirectoryLock(
        queryId,
        lock,
        () -> {
          if (lock.isClosing()) {
            return null;
          }
          IOException failure = closeReaders(queryId, ownerDirectory);
          failure = mergeFailures(failure, closeSpillers(queryId, ownerDirectory));
          if (failure != null) {
            throw failure;
          }
          deleteDirectoryIfExists(ownerDirectory);
          Set<Path> owners = queryDirectories.get(queryId);
          if (owners != null && owners.remove(ownerDirectory) && owners.isEmpty()) {
            try {
              deleteDirectoryIfExists(resolveQueryDirectory(queryId));
            } catch (IOException e) {
              owners.add(ownerDirectory);
              throw e;
            }
            queryDirectories.remove(queryId, owners);
          }
          return null;
        },
        true,
        true);
  }

  public void deregisterQuery(String queryId) throws IOException {
    QueryDirectoryLock lock =
        queryDirectoryLocks.computeIfAbsent(queryId, ignored -> new QueryDirectoryLock());
    if (!lock.tryAcquireReference()) {
      return;
    }
    boolean closed =
        withQueryDirectoryLock(
            queryId,
            lock,
            () -> {
              if (lock.isClosing()) {
                return false;
              }
              IOException failure = closeReaders(queryId, null);
              failure = mergeFailures(failure, closeSpillers(queryId, null));
              if (failure != null) {
                throw failure;
              }
              deleteDirectoryIfExists(resolveQueryDirectory(queryId));
              queryDirectories.remove(queryId);
              lock.markClosing();
              return true;
            },
            true,
            true);
    if (closed) {
      lock.awaitNoReferences();
      queryDirectoryLocks.remove(queryId, lock);
    }
  }

  public byte[] readSegment(String queryId, String planNodeId, int segmentId) throws IOException {
    return withQueryDirectoryReadLock(
        queryId,
        () -> Files.readAllBytes(getRegisteredSegmentPath(queryId, planNodeId, segmentId)));
  }

  public Path resolveSegment(String queryId, String planNodeId, int segmentId) throws IOException {
    return withQueryDirectoryReadLock(
        queryId,
        () -> {
          Path segment = getRegisteredSegmentPath(queryId, planNodeId, segmentId);
          if (!Files.isRegularFile(segment)) {
            throw new NoSuchFileException(segment.toString());
          }
          return segment;
        });
  }

  public void deleteSegment(String queryId, String dataSetId, int segmentId) throws IOException {
    QueryDirectoryLock lock = tryAcquireQueryDirectoryLock(queryId);
    if (lock == null) {
      return;
    }
    // Keep segment deletion exclusive with readers and query cleanup.
    withQueryDirectoryLock(
        queryId,
        lock,
        () -> {
          if (lock.isClosing()) {
            return null;
          }
          Path file = null;
          try {
            file = getRegisteredSegmentPath(queryId, dataSetId, segmentId);
            Files.deleteIfExists(file);
          } catch (NoSuchFileException | AccessDeniedException e) {
            // Query cleanup may have already removed the whole query directory.
            LOGGER.warn(
                String.format(
                    DataNodeQueryMessages
                        .LOG_FAILED_TO_CLEAN_DEVICEENTRY_SPILL_DIRECTORY_FOR_QUERY_ARG_53D9C1FC,
                    file));
          }
          return null;
        },
        true,
        true);
  }

  public void deleteSegment(String queryId, PlanNodeId planNodeId, int segmentId)
      throws IOException {
    deleteSegment(queryId, planNodeId.getId(), segmentId);
  }

  public void finishSegmentDataSet(String queryId, String planNodeId) throws IOException {
    Path ownerDirectory = resolveOwnerDirectory(queryId, planNodeId);
    deregisterOwner(queryId, ownerDirectory);
  }

  private Path resolveRegisteredDataSetDirectory(String queryId, String planNodeId)
      throws IOException {
    Path relativeDataSetPath = Path.of(planNodeId);
    Path queryDirectory = resolveQueryDirectory(queryId);
    Path dataSetDirectory = queryDirectory.resolve(relativeDataSetPath).resolve("fi").normalize();
    if (!dataSetDirectory.startsWith(queryDirectory)) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_DEVICEENTRY_DATA_SET_PATH_ESCAPES_THE_QUERY_DIRECTORY_ARG_394A9840,
              dataSetDirectory));
    }
    Set<Path> owners = queryDirectories.get(queryId);
    boolean registered =
        owners != null
            && owners.stream()
                .map(Path::normalize)
                .anyMatch(owner -> dataSetDirectory.startsWith(owner) && Files.isDirectory(owner));
    if (!registered || !Files.isDirectory(dataSetDirectory)) {
      throw new NoSuchFileException(dataSetDirectory.toString());
    }
    return dataSetDirectory;
  }

  private Path getRegisteredSegmentPath(String queryId, String planNodeId, int segmentId)
      throws IOException {
    if (segmentId < 0) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_DEVICEENTRY_SEGMENT_ID_MUST_BE_NON_NEGATIVE_ARG_F7653A57,
              segmentId));
    }
    return resolveRegisteredDataSetDirectory(queryId, planNodeId)
        .resolve(String.format("segment-%06d.bin", segmentId));
  }

  @TestOnly
  public void clearStaleData() throws IOException {
    deleteDirectoryIfExists(rootDirectory());
    Files.createDirectories(rootDirectory());
    queryDirectories.clear();
    querySpillers.clear();
    queryReaders.clear();
    queryDirectoryLocks.clear();
  }

  DeviceEntryDiskSpiller createSpiller(
      String queryId,
      Path ownerDirectory,
      Path directory,
      long targetSegmentBytes,
      DeviceEntryIOContext ioContext,
      boolean duringFetchSchema)
      throws IOException {
    return withQueryDirectoryReadLock(
        queryId,
        () -> {
          checkOwnerRegistered(queryId, ownerDirectory);
          DeviceEntryDiskSpiller spiller =
              new DeviceEntryDiskSpiller(
                  this,
                  queryId,
                  ownerDirectory,
                  directory,
                  targetSegmentBytes,
                  ioContext,
                  duringFetchSchema);
          querySpillers.compute(
              queryId,
              (ignored, spillers) -> {
                Set<DeviceEntryDiskSpiller> registeredSpillers =
                    spillers == null ? ConcurrentHashMap.newKeySet() : spillers;
                registeredSpillers.add(spiller);
                return registeredSpillers;
              });
          return spiller;
        });
  }

  DeviceEntryFileSpillerReader createReader(
      String queryId,
      Path ownerDirectory,
      java.util.List<Path> segments,
      boolean deleteSegmentAfterRead,
      DeviceEntryIOContext ioContext,
      boolean duringFetchSchema)
      throws IOException {
    return withQueryDirectoryReadLock(
        queryId,
        () -> {
          checkOwnerRegistered(queryId, ownerDirectory);
          DeviceEntryFileSpillerReader reader =
              new DeviceEntryFileSpillerReader(
                  this,
                  queryId,
                  ownerDirectory,
                  segments,
                  deleteSegmentAfterRead,
                  ioContext,
                  duringFetchSchema);
          queryReaders.compute(
              queryId,
              (ignored, readers) -> {
                Set<DeviceEntryFileSpillerReader> registeredReaders =
                    readers == null ? ConcurrentHashMap.newKeySet() : readers;
                registeredReaders.add(reader);
                return registeredReaders;
              });
          return reader;
        });
  }

  void appendSpiller(DeviceEntryDiskSpiller spiller, byte[] serializedEntry) throws IOException {
    withQueryDirectoryReadLock(
        spiller.queryId(),
        () -> {
          checkSpillerRegistered(spiller);
          spiller.appendInternal(serializedEntry);
          return null;
        });
  }

  java.util.List<Path> finishSpiller(DeviceEntryDiskSpiller spiller) throws IOException {
    return withQueryDirectoryReadLock(
        spiller.queryId(),
        () -> {
          checkSpillerRegistered(spiller);
          return spiller.finishInternal();
        });
  }

  void closeSpiller(DeviceEntryDiskSpiller spiller) throws IOException {
    QueryDirectoryLock lock = tryAcquireQueryDirectoryLock(spiller.queryId());
    if (lock == null) {
      spiller.closeInternal();
      removeSpiller(spiller);
      return;
    }
    withQueryDirectoryLock(
        spiller.queryId(),
        lock,
        () -> {
          spiller.closeInternal();
          removeSpiller(spiller);
          return null;
        },
        false,
        true);
  }

  boolean hasNextReader(DeviceEntryFileSpillerReader reader) throws IOException {
    return withQueryDirectoryReadLock(
        reader.queryId(),
        () -> {
          checkReaderRegistered(reader);
          return reader.hasNextInternal();
        });
  }

  org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry nextReader(
      DeviceEntryFileSpillerReader reader) throws IOException {
    return withQueryDirectoryReadLock(
        reader.queryId(),
        () -> {
          checkReaderRegistered(reader);
          return reader.nextInternal();
        });
  }

  void closeReader(DeviceEntryFileSpillerReader reader) throws IOException {
    QueryDirectoryLock lock = tryAcquireQueryDirectoryLock(reader.queryId());
    if (lock == null) {
      reader.closeInternal();
      removeReader(reader);
      return;
    }
    withQueryDirectoryLock(
        reader.queryId(),
        lock,
        () -> {
          reader.closeInternal();
          removeReader(reader);
          return null;
        },
        false,
        true);
  }

  <T> T withQueryDirectoryReadLock(String queryId, IOCallable<T> operation) throws IOException {
    return withQueryDirectoryLock(queryId, operation, false);
  }

  <T> T withQueryDirectoryReadLock(String queryId, IOCallable<T> operation, boolean createIfAbsent)
      throws IOException {
    QueryDirectoryLock lock =
        createIfAbsent
            ? queryDirectoryLocks.computeIfAbsent(queryId, ignored -> new QueryDirectoryLock())
            : tryAcquireQueryDirectoryLock(queryId);
    if (createIfAbsent && !lock.tryAcquireReference()) {
      throw new NoSuchFileException(resolveQueryDirectory(queryId).toString());
    }
    if (lock == null) {
      throw new NoSuchFileException(resolveQueryDirectory(queryId).toString());
    }
    return withQueryDirectoryLock(queryId, lock, operation, false, false);
  }

  private <T> T withQueryDirectoryLock(String queryId, IOCallable<T> operation, boolean exclusive)
      throws IOException {
    QueryDirectoryLock lock = tryAcquireQueryDirectoryLock(queryId);
    if (lock == null) {
      throw new NoSuchFileException(resolveQueryDirectory(queryId).toString());
    }
    return withQueryDirectoryLock(queryId, lock, operation, exclusive, false);
  }

  private <T> T withQueryDirectoryLock(
      String queryId,
      QueryDirectoryLock lock,
      IOCallable<T> operation,
      boolean exclusive,
      boolean allowClosing)
      throws IOException {
    Lock acquiredLock = exclusive ? lock.lock.writeLock() : lock.lock.readLock();
    acquiredLock.lock();
    try {
      if (!allowClosing && lock.isClosing()) {
        throw new NoSuchFileException(resolveQueryDirectory(queryId).toString());
      }
      return operation.call();
    } finally {
      acquiredLock.unlock();
      lock.releaseReference();
    }
  }

  private QueryDirectoryLock tryAcquireQueryDirectoryLock(String queryId) {
    QueryDirectoryLock lock = queryDirectoryLocks.get(queryId);
    return lock != null && lock.tryAcquireReference() ? lock : null;
  }

  void checkOwnerRegistered(String queryId, Path ownerDirectory) throws IOException {
    Set<Path> owners = queryDirectories.get(queryId);
    if (owners == null || !owners.contains(ownerDirectory)) {
      throw new NoSuchFileException(ownerDirectory.toString());
    }
  }

  private void checkSpillerRegistered(DeviceEntryDiskSpiller spiller) throws IOException {
    Set<DeviceEntryDiskSpiller> spillers = querySpillers.get(spiller.queryId());
    if (spillers == null || !spillers.contains(spiller)) {
      throw new NoSuchFileException(spiller.ownerDirectory().toString());
    }
    checkOwnerRegistered(spiller.queryId(), spiller.ownerDirectory());
  }

  private void checkReaderRegistered(DeviceEntryFileSpillerReader reader) throws IOException {
    Set<DeviceEntryFileSpillerReader> readers = queryReaders.get(reader.queryId());
    if (readers == null || !readers.contains(reader)) {
      throw new NoSuchFileException(reader.ownerDirectory().toString());
    }
    checkOwnerRegistered(reader.queryId(), reader.ownerDirectory());
  }

  private IOException closeReaders(String queryId, Path ownerDirectory) {
    Set<DeviceEntryFileSpillerReader> readers = queryReaders.get(queryId);
    if (readers == null) {
      return null;
    }
    IOException failure = null;
    for (DeviceEntryFileSpillerReader reader : Set.copyOf(readers)) {
      if (ownerDirectory == null || ownerDirectory.equals(reader.ownerDirectory())) {
        try {
          reader.closeInternal();
          readers.remove(reader);
        } catch (IOException e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
    }
    if (readers.isEmpty()) {
      queryReaders.remove(queryId, readers);
    }
    return failure;
  }

  private IOException closeSpillers(String queryId, Path ownerDirectory) {
    Set<DeviceEntryDiskSpiller> spillers = querySpillers.get(queryId);
    if (spillers == null) {
      return null;
    }
    IOException failure = null;
    for (DeviceEntryDiskSpiller spiller : Set.copyOf(spillers)) {
      if (ownerDirectory == null || ownerDirectory.equals(spiller.ownerDirectory())) {
        try {
          spiller.closeInternal();
          spillers.remove(spiller);
        } catch (IOException e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
    }
    if (spillers.isEmpty()) {
      querySpillers.remove(queryId, spillers);
    }
    return failure;
  }

  private IOException mergeFailures(IOException first, IOException next) {
    if (first == null) {
      return next;
    }
    if (next != null) {
      first.addSuppressed(next);
    }
    return first;
  }

  private void removeSpiller(DeviceEntryDiskSpiller spiller) {
    querySpillers.computeIfPresent(
        spiller.queryId(),
        (ignored, spillers) -> {
          spillers.remove(spiller);
          return spillers.isEmpty() ? null : spillers;
        });
  }

  private void removeReader(DeviceEntryFileSpillerReader reader) {
    queryReaders.computeIfPresent(
        reader.queryId(),
        (ignored, readers) -> {
          readers.remove(reader);
          return readers.isEmpty() ? null : readers;
        });
  }

  private void deleteDirectoryIfExists(Path directory) throws IOException {
    // Avoid most concurrent conflicts when query with device entry spilled fails
    if (!Files.exists(directory)) {
      return;
    }

    Files.walkFileTree(
        directory,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
              throws IOException {
            try {
              Files.deleteIfExists(file);
            } catch (NoSuchFileException e) {
              // Another cleanup may have deleted this file.
              LOGGER.warn(
                  String.format(
                      DataNodeQueryMessages
                          .LOG_FAILED_TO_CLEAN_DEVICEENTRY_SPILL_DIRECTORY_FOR_QUERY_ARG_53D9C1FC,
                      file));
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exception)
              throws IOException {
            if (exception instanceof NoSuchFileException) {
              LOGGER.warn(
                  String.format(
                      DataNodeQueryMessages
                          .LOG_FAILED_TO_CLEAN_DEVICEENTRY_SPILL_DIRECTORY_FOR_QUERY_ARG_53D9C1FC,
                      file));
              return FileVisitResult.CONTINUE;
            }
            throw exception;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exception)
              throws IOException {
            if (exception != null && !(exception instanceof NoSuchFileException)) {
              throw exception;
            }
            try {
              Files.deleteIfExists(dir);
            } catch (NoSuchFileException e) {
              // Another cleanup may have deleted this directory.
              LOGGER.warn(
                  String.format(
                      DataNodeQueryMessages
                          .LOG_FAILED_TO_CLEAN_DEVICEENTRY_SPILL_DIRECTORY_FOR_QUERY_ARG_53D9C1FC,
                      dir));
            }
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private Path rootDirectory() {
    return Path.of(IoTDBDescriptor.getInstance().getConfig().getSortTmpDir(), "device-entry");
  }

  private Path resolveOwnerDirectory(String queryId, String planNodeId) {
    Path queryDirectory = resolveQueryDirectory(queryId);
    Path ownerDirectory = queryDirectory.resolve(planNodeId).normalize();
    return ownerDirectory;
  }

  private Path resolveQueryDirectory(String queryId) {
    Path root = rootDirectory().normalize();
    Path queryDirectory = root.resolve(queryId).normalize();
    return queryDirectory;
  }

  @FunctionalInterface
  interface IOCallable<T> {
    T call() throws IOException;
  }

  private static final class QueryDirectoryLock {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final AtomicInteger references = new AtomicInteger();
    private final Object referenceMonitor = new Object();
    private volatile boolean closing;

    private boolean tryAcquireReference() {
      if (closing) {
        return false;
      }
      references.incrementAndGet();
      if (!closing) {
        return true;
      }
      releaseReference();
      return false;
    }

    private void releaseReference() {
      if (references.decrementAndGet() == 0) {
        synchronized (referenceMonitor) {
          referenceMonitor.notifyAll();
        }
      }
    }

    private void markClosing() {
      closing = true;
    }

    private boolean isClosing() {
      return closing;
    }

    private void awaitNoReferences() {
      boolean interrupted = false;
      synchronized (referenceMonitor) {
        while (references.get() != 0) {
          try {
            referenceMonitor.wait();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static class DeviceEntrySpillManagerHolder {
    private static final DeviceEntrySpillManager INSTANCE = new DeviceEntrySpillManager();

    private DeviceEntrySpillManagerHolder() {}
  }
}
