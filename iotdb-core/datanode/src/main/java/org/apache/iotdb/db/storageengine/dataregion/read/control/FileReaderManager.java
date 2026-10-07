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
import java.util.function.LongConsumer;

/** Manages cached readers with independent locks for independent files. */
public class FileReaderManager {
  private static final Logger logger = LoggerFactory.getLogger(FileReaderManager.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  private static final int MAX_CACHED_FILE_SIZE = 30000;
  private static final int PRINT_INTERVAL = 10000;

  // registryLock protects entry lifetime and reference counts; entry monitors serialize reader I/O.
  // Never hold registryLock while taking an entry monitor or doing I/O. An entry may briefly take
  // registryLock to recheck references, but must not acquire resource, region, or other entry
  // locks.
  // Reference registration takes only registryLock, even when the caller holds a region read lock.
  private final Object registryLock = new Object();
  private final Object clearLock = new Object();
  private final Map<TsFileID, Entry> internal = new HashMap<>();
  private final Map<String, Entry> external = new HashMap<>();
  private final AtomicInteger closedCount = new AtomicInteger();
  private final AtomicInteger unclosedCount = new AtomicInteger();
  private final AtomicInteger externalCount = new AtomicInteger();
  // Guarded by registryLock. Pins count operations (including entry-lock waiters), not queries.
  private boolean clearing;
  private int pins;

  private FileReaderManager() {}

  public static FileReaderManager getInstance() {
    return FileReaderManagerHelper.INSTANCE;
  }

  private static class Slot {
    private TsFileSequenceReader reader;
    // Guarded by registryLock, independently of this slot's reader I/O.
    private int references;
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

  private static class Entry {
    private final Slot closed;
    private final Slot unclosed;
    // Guarded by registryLock; reader fields are guarded by this entry's monitor.
    private int pins;

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
          slot.reader =
              isClosed
                  ? new TsFileSequenceReader(
                      path, recorder, EncryptDBUtils.getFirstEncryptParamFromTSFilePath(path))
                  : new UnClosedTsFileReader(
                      path, EncryptDBUtils.getFirstEncryptParamFromTSFilePath(path), recorder);
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
        // Preserve the legacy unclosed -> closed fallback when no unclosed ref is registered.
        slot = !isClosed && entry.unclosed.references != 0 ? entry.unclosed : entry.closed;
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
        try {
          closeSlot(slot, StorageEngineMessages.TSFILE_READER_CLOSED_BECAUSE_NO_REFERENCE);
        } catch (IOException e) {
          // Slot.close already logged the error and discarded the unusable handle.
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

  // Called with registryLock held; wait releases it so operations can finish and unpin.
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
