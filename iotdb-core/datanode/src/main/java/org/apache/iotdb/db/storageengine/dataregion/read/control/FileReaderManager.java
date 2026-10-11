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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.UnClosedTsFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * Caches readers for managed TsFiles and external file paths, with reader I/O serialized per file.
 *
 * <p>Each {@link Entry} is a stable lifecycle record and the monitor for its reader slots. An
 * operation pin keeps the entry registered while a manager call uses it or waits for its monitor.
 * Pins do not keep a returned reader open: callers retain a {@link Slot#references file-reader
 * reference} for the duration of reader use, separately from each manager call's pin.
 *
 * <p>{@link #registryLock} protects registry membership, pins, and reference counts. Entry monitors
 * protect reader construction, close, and the cached reader fields, not reads performed by callers
 * after {@code get} returns. Never hold {@code registryLock} while acquiring an entry monitor or
 * doing I/O. An entry may briefly acquire {@code registryLock} to recheck references, but must not
 * acquire a resource lock, region lock, or another entry monitor. Reference registration uses only
 * the short registry critical section, so callers can register while holding a region read lock.
 */
public class FileReaderManager {
  private static final Logger logger = LoggerFactory.getLogger(FileReaderManager.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  private static final int MAX_CACHED_FILE_SIZE = 30000;
  private static final int PRINT_INTERVAL = 10000;

  /**
   * Manager-wide monitor for both registries, {@link #clearing}, both levels of pin counters, and
   * {@link Slot#references}. It is never held across reader I/O or an entry-monitor acquisition.
   */
  private final Object registryLock = new Object();

  /** Serializes entire global test-cleanup passes; ordinary reader operations do not acquire it. */
  private final Object clearLock = new Object();

  /**
   * Managed files keyed by TsFileID, each with independent closed/unclosed slots under one entry
   * monitor. Guarded by {@link #registryLock}.
   */
  private final Map<TsFileID, Entry> internal = new HashMap<>();

  /**
   * External files keyed by the supplied path string, in a namespace separate from {@link
   * #internal}. External entries use only their closed slot. Guarded by {@link #registryLock}.
   */
  private final Map<String, Entry> external = new HashMap<>();

  /** Cached closed-reader count for managed files; updated concurrently under different entries. */
  private final AtomicInteger closedCount = new AtomicInteger();

  /** Cached unclosed-reader count for managed files; independent of query references and pins. */
  private final AtomicInteger unclosedCount = new AtomicInteger();

  /** Cached-reader count for the external-path namespace. */
  private final AtomicInteger externalCount = new AtomicInteger();

  /**
   * Test-cleanup admission gate: new acquisitions wait while releases remain allowed. Guarded by
   * {@link #registryLock}; it remains set through the final pin drain and entry reclamation.
   */
  private boolean clearing;

  /**
   * Sum of {@link Entry#pins} across both registries, including operations waiting for entry
   * monitors. Global test cleanup drains this counter; zero does not imply zero query references.
   * Guarded by {@link #registryLock}.
   */
  private int pins;

  private final AtomicLong openReaderCost = new AtomicLong(0);
  private final AtomicLong openReaderCount = new AtomicLong(0);

  private final AtomicLong closeReaderCost = new AtomicLong(0);
  private final AtomicLong closeReaderCount = new AtomicLong(0);

  private FileReaderManager() {}

  public static FileReaderManager getInstance() {
    return FileReaderManagerHelper.INSTANCE;
  }

  /** One reader state (closed or unclosed) and its independently maintained query references. */
  private static class Slot {
    /**
     * Nullable cached handle, created and discarded under the owning entry's monitor. Reclamation
     * may inspect it under registryLock after that entry's operations have finished.
     */
    private TsFileSequenceReader reader;

    /**
     * Outstanding file-reader retains for this slot, guarded by registryLock. May be positive
     * before any reader is opened; unlike operation pins, these retains span the caller's reader
     * use.
     */
    private int references;

    /**
     * Shared cached-reader counter for this slot's namespace/state. Publishing a reader increments
     * it; discarding a handle after a close attempt decrements it, even when close fails. This
     * counts cached handles, not query references or all open OS descriptors.
     */
    private final AtomicInteger count;

    private Slot(AtomicInteger count) {
      this.count = count;
    }

    private void close() throws IOException {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error(
              StorageEngineMessages.CANNOT_CLOSE_TSFILE_SEQUENCE_READER, reader.getFileName(), e);
          throw e;
        } finally {
          // A failed close must neither poison future reads nor retain an unused entry forever.
          // As with the original manager, discard the handle after a best-effort close. Pins keep
          // this entry stable until the attempt finishes, including when close throws unchecked.
          reader = null;
          count.decrementAndGet();
        }
      }
    }

    private boolean empty() {
      return reader == null && references == 0;
    }
  }

  /**
   * Stable per-key record whose monitor serializes reader lifecycle changes. It cannot be reclaimed
   * while an operation is pinned, even if both slots are empty, because a waiter may still use this
   * exact monitor. Replacing it early would allow two entries to open readers for the same key.
   */
  private static class Entry {
    /** Closed-file reader slot; also the sole slot used by external entries. */
    private final Slot closed;

    /** Unclosed-file reader slot for managed files; unused for external entries. */
    private final Slot unclosed;

    /**
     * Manager operations retaining this entry, including monitor waiters; guarded by registryLock.
     */
    private int pins;

    /**
     * @param closedCount managed closed-reader counter or external-reader counter, depending on the
     *     registry that owns this entry
     * @param unclosedCount managed unclosed-reader counter; external entries never use this slot
     */
    private Entry(AtomicInteger closedCount, AtomicInteger unclosedCount) {
      closed = new Slot(closedCount);
      unclosed = new Slot(unclosedCount);
    }

    private Slot slot(boolean isClosed) {
      return isClosed ? closed : unclosed;
    }

    private boolean empty() {
      return closed.empty() && unclosed.empty();
    }
  }

  /**
   * Looks up an entry and retains its identity for one manager operation, optionally creating it.
   *
   * <p>The operation is counted before the caller can wait for the entry monitor, so reclamation
   * cannot replace an entry that a waiter still holds. This method neither acquires that monitor,
   * opens a reader, nor registers a query reference. Every non-null result must be paired with
   * exactly one {@code unpin} in a {@code finally} block, after leaving the entry monitor.
   *
   * <p>Admission waits are uninterruptible: an interrupt is remembered and restored before this
   * method returns. Releases bypass the gate so they can complete during global test cleanup.
   *
   * @param <K> TsFileID for the internal registry, or String for the external registry
   * @param registry exactly {@link #internal} or {@link #external}; also selects the cached-reader
   *     counter for a newly created entry
   * @param key non-null file identity within the selected registry
   * @param admission true to wait until global test cleanup reopens admission; false for reference
   *     releases that must remain allowed during cleanup
   * @param create true to install an empty entry if the key is absent; false to return null instead
   * @return the entry with both its own and the global operation-pin count incremented, or null if
   *     the key is absent and creation is disabled
   */
  private <K> Entry pin(Map<K, Entry> registry, K key, boolean admission, boolean create) {
    Objects.requireNonNull(key);
    boolean interrupted = false;
    synchronized (registryLock) {
      while (admission && clearing) {
        try {
          registryLock.wait();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      Entry entry = registry.get(key);
      if (entry == null && create) {
        entry = new Entry(registry == internal ? closedCount : externalCount, unclosedCount);
        registry.put(key, entry);
      }
      if (entry != null) {
        entry.pins++;
        pins++;
      }
      return entry;
    }
  }

  /**
   * Releases the matching operation pin without changing query references or closing a reader.
   * Outside global cleanup, an empty entry is reclaimed only when no operation still retains it.
   * Call after leaving the entry monitor, including when the operation fails.
   *
   * @param <K> key type of the selected registry
   * @param registry the same registry supplied to the matching {@code pin}
   * @param key the same file identity supplied to the matching {@code pin}
   * @param entry the exact non-null entry returned by that call, not a fresh registry lookup
   */
  private <K> void unpin(Map<K, Entry> registry, K key, Entry entry) {
    synchronized (registryLock) {
      entry.pins--;
      pins--;
      // At zero pins all slot mutations have completed and published through registryLock.
      // Keep entries stable during clear, including those used by releases entering mid-clear.
      if (!clearing && entry.pins == 0 && entry.empty()) {
        registry.remove(key, entry);
      }
      if (clearing && pins == 0) {
        registryLock.notifyAll();
      }
    }
  }

  /** Caller must exclude new references and finish existing reader use (resource write lock). */
  public void closeFileAndRemoveReader(TsFileID tsFileID) throws IOException {
    Entry entry = pin(internal, tsFileID, true, false);
    if (entry == null) {
      return;
    }
    try {
      synchronized (entry) {
        closeEntry(entry);
      }
    } finally {
      unpin(internal, tsFileID, entry);
    }
  }

  private void closeEntry(Entry entry) throws IOException {
    synchronized (registryLock) {
      entry.closed.references = 0;
      entry.unclosed.references = 0;
    }
    IOException failure = null;
    try {
      try {
        closeSlot(entry.closed, StorageEngineMessages.CLOSED_TSFILE_READER_CLOSED);
      } catch (IOException e) {
        failure = mergeFailure(failure, e);
      }
    } finally {
      // Also attempt the second slot if the first close throws an unchecked exception.
      try {
        closeSlot(entry.unclosed, StorageEngineMessages.UNCLOSED_TSFILE_READER_CLOSED);
      } catch (IOException e) {
        failure = mergeFailure(failure, e);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static void closeSlot(Slot slot, String message) throws IOException {
    TsFileSequenceReader reader = slot.reader;
    slot.close();
    if (reader != null && resourceLogger.isDebugEnabled()) {
      resourceLogger.debug(message, reader.getFileName());
    }
  }

  private static IOException mergeFailure(IOException failure, IOException next) {
    if (failure == null) {
      return new IOException(next);
    }
    // Never attach other files' errors to an exception supplied by a reader.
    failure.addSuppressed(next);
    return failure;
  }

  public TsFileSequenceReader get(String filePath, TsFileID tsFileID, boolean isClosed)
      throws IOException {
    return get(filePath, tsFileID, isClosed, null);
  }

  public TsFileSequenceReader get(
      String filePath, TsFileID tsFileID, boolean isClosed, LongConsumer ioSizeRecorder)
      throws IOException {
    return get(filePath, tsFileID, isClosed, ioSizeRecorder, false);
  }

  /** Get does not acquire a query reference. A failed constructor leaves registered refs intact. */
  public TsFileSequenceReader get(
      String filePath,
      TsFileID tsFileID,
      boolean isClosed,
      LongConsumer ioSizeRecorder,
      boolean isExternalTsFile)
      throws IOException {
    return isExternalTsFile
        ? get(external, filePath, filePath, true, ioSizeRecorder)
        : get(internal, tsFileID, filePath, isClosed, ioSizeRecorder);
  }

  @SuppressWarnings("squid:S2095")
  private <K> TsFileSequenceReader get(
      Map<K, Entry> registry, K key, String path, boolean isClosed, LongConsumer recorder)
      throws IOException {
    Entry entry = pin(registry, key, true, true);
    try {
      synchronized (entry) {
        Slot slot = entry.slot(isClosed);
        if (slot.reader == null) {
          long startTime = System.nanoTime();
          slot.reader =
              isClosed
                  ? new TsFileSequenceReader(
                      path, recorder, EncryptDBUtils.getFirstEncryptParamFromTSFilePath(path))
                  : new UnClosedTsFileReader(
                      path, EncryptDBUtils.getFirstEncryptParamFromTSFilePath(path), recorder);
          openReaderCount.incrementAndGet();
          openReaderCost.addAndGet(System.nanoTime() - startTime);
          int count = slot.count.incrementAndGet();
          if (count >= MAX_CACHED_FILE_SIZE && count % PRINT_INTERVAL == 0) {
            logger.warn(StorageEngineMessages.QUERY_OPENED_FILES, count);
          }
        }
        return slot.reader;
      }
    } finally {
      unpin(registry, key, entry);
    }
  }

  /**
   * Retains one reader reference and the resource read lock until the matching release.
   *
   * @param tsFile resource whose reader will be used
   * @param isClosed slot to retain; pass this same value when releasing, even if the file is sealed
   *     in the meantime
   */
  public void increaseFileReaderReference(TsFileResource tsFile, boolean isClosed) {
    tsFile.readLock();
    boolean registered = false;
    try {
      increase(internal, tsFile.getTsFileID(), isClosed);
      registered = true;
    } finally {
      if (!registered) {
        tsFile.readUnlock();
      }
    }
  }

  public void increaseExternalFileReaderReference(String filePath) {
    increase(external, filePath, true);
  }

  private <K> void increase(Map<K, Entry> registry, K key, boolean isClosed) {
    // One short registry critical section, with no wait for an opening or closing reader.
    synchronized (registryLock) {
      Entry entry = pin(registry, key, true, true);
      try {
        entry.slot(isClosed).references++;
      } finally {
        unpin(registry, key, entry);
      }
    }
  }

  /**
   * Releases one retained reference and its resource read lock. The resource lock is released even
   * if global test cleanup has already removed the reader reference.
   *
   * @param tsFile resource from the matching successful registration; release it exactly once
   * @param isClosed slot selected at registration, not the file's current sealed state
   */
  public void decreaseFileReaderReference(TsFileResource tsFile, boolean isClosed) {
    try {
      decrease(internal, tsFile.getTsFileID(), isClosed);
    } finally {
      tsFile.readUnlock();
    }
  }

  public void decreaseExternalFileReaderReference(String filePath) {
    decrease(external, filePath, true);
  }

  private <K> void decrease(Map<K, Entry> registry, K key, boolean isClosed) {
    Entry entry = pin(registry, key, false, false);
    if (entry == null) {
      return;
    }
    try {
      Slot slot;
      synchronized (registryLock) {
        // Sealing a file does not migrate existing references between slots.
        slot = entry.slot(isClosed);
        if (slot.references == 0 || --slot.references != 0) {
          return;
        }
      }
      synchronized (entry) {
        synchronized (registryLock) {
          // A new query may have registered while this release waited for reader I/O.
          if (slot.references != 0) {
            return;
          }
        }
        // New references may now register, but their get() waits for this entry's close to finish.
        long startTime = System.nanoTime();
        try {
          closeSlot(
              slot,
              slot == entry.closed
                  ? StorageEngineMessages
                      .LOG_READER_FOR_CLOSED_TSFILE_ARG_IS_CLOSED_BECAUSE_ITS_REFERENCE_COUNT_REACHED_ZERO_C3B71A85
                  : StorageEngineMessages
                      .LOG_READER_FOR_UNCLOSED_TSFILE_ARG_IS_CLOSED_BECAUSE_ITS_REFERENCE_COUNT_REACHED_ZERO_088BDEF8);
        } catch (IOException e) {
          // Slot.close already logged the error and discarded the unusable handle.
        } finally {
          closeReaderCount.incrementAndGet();
          closeReaderCost.addAndGet(System.nanoTime() - startTime);
        }
      }
    } finally {
      unpin(registry, key, entry);
    }
  }

  /**
   * Test cleanup only: callers must first stop reader use. Zero operation pins does not mean zero
   * query references. Acquisitions wait; releases remain admitted throughout cleanup. Interrupts
   * are restored only after cleanup and the final release-pin drain, never reopening the gate
   * early.
   */
  @TestOnly
  public void closeAndRemoveAllOpenedReaders() throws IOException {
    synchronized (clearLock) {
      boolean interrupted = Thread.interrupted();
      IOException failure = null;
      try {
        List<Entry> entries;
        synchronized (registryLock) {
          clearing = true;
          interrupted |= drainPins();
          entries = new ArrayList<>(internal.values());
          entries.addAll(external.values());
        }
        for (Entry entry : entries) {
          synchronized (entry) {
            try {
              closeEntry(entry);
            } catch (IOException e) {
              failure = mergeFailure(failure, e);
            }
          }
          interrupted |= Thread.interrupted();
        }
      } finally {
        try {
          synchronized (registryLock) {
            try {
              // Drain and reclaim before reopening admission: releases may still retain an entry.
              interrupted |= Thread.interrupted();
              interrupted |= drainPins();
              internal.values().removeIf(Entry::empty);
              external.values().removeIf(Entry::empty);
            } finally {
              // Even a failed snapshot or reclamation must not permanently close the test gate.
              clearing = false;
              registryLock.notifyAll();
            }
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  /**
   * Waits for all currently admitted manager operations to finish during global test cleanup.
   * Requires registryLock; waiting releases that monitor so operations can finish and unpin. This
   * drains operation pins, not query references, and does not acquire any entry monitor.
   *
   * @return whether an interrupt was consumed while waiting; the cleanup caller restores it after
   *     completing cleanup and reopening admission
   */
  private boolean drainPins() {
    boolean interrupted = false;
    while (pins != 0) {
      try {
        registryLock.wait();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    return interrupted;
  }

  @TestOnly
  public boolean contains(TsFileResource tsFile, boolean isClosed) {
    TsFileID key = tsFile.getTsFileID();
    Entry entry = pin(internal, key, true, false);
    if (entry == null) {
      return false;
    }
    try {
      synchronized (entry) {
        return entry.slot(isClosed).reader != null;
      }
    } finally {
      unpin(internal, key, entry);
    }
  }

  /** Snapshot for tests; modifying it does not mutate the manager. */
  @TestOnly
  public Map<TsFileID, TsFileSequenceReader> getClosedFileReaderMap() {
    return snapshot(true);
  }

  @TestOnly
  public Map<TsFileID, TsFileSequenceReader> getUnclosedFileReaderMap() {
    return snapshot(false);
  }

  private Map<TsFileID, TsFileSequenceReader> snapshot(boolean isClosed) {
    List<TsFileID> keys;
    synchronized (registryLock) {
      keys = new ArrayList<>(internal.keySet());
    }
    Map<TsFileID, TsFileSequenceReader> result = new HashMap<>();
    for (TsFileID key : keys) {
      Entry entry = pin(internal, key, true, false);
      if (entry != null) {
        try {
          synchronized (entry) {
            if (entry.slot(isClosed).reader != null) {
              result.put(key, entry.slot(isClosed).reader);
            }
          }
        } finally {
          unpin(internal, key, entry);
        }
      }
    }
    return result;
  }

  public void printReaderCost() {
    logger.info(
        "openReaderCount: {}, openReaderCost: {}, closeReaderCount: {}, closeReaderCost: {}",
        openReaderCount.get(),
        openReaderCost.get(),
        closeReaderCount.get(),
        closeReaderCost.get());
  }

  @TestOnly
  public void setReaderForTest(TsFileID key, boolean isClosed, TsFileSequenceReader reader)
      throws IOException {
    Entry entry = pin(internal, key, true, true);
    try {
      synchronized (entry) {
        Slot slot = entry.slot(isClosed);
        slot.close();
        slot.reader = reader;
        if (reader != null) {
          slot.count.incrementAndGet();
        }
      }
    } finally {
      unpin(internal, key, entry);
    }
  }

  private static class FileReaderManagerHelper {
    private static final FileReaderManager INSTANCE = new FileReaderManager();

    private FileReaderManagerHelper() {}
  }
}
