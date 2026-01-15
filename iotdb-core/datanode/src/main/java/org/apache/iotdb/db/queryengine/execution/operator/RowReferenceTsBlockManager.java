/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArrayFIFOQueue;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;
import java.util.HashSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;

public class RowReferenceTsBlockManager {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RowReferenceTsBlockManager.class);
  private static final long TSBLOCK_ACCOUNTING_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TsBlockAccounting.class);
  private static final int RESERVED_ROW_ID_FOR_CURSOR = -1;

  private final IdRegistry<TsBlockAccounting> tsBlocks = new IdRegistry<>();
  private final RowIdBuffer rowIdBuffer = new RowIdBuffer();
  private final HashSet<Integer> compactionCandidates = new HashSet<>();

  private LoadCursor currentCursor;
  private long tsBlockBytes;

  public LoadCursor add(TsBlock tsBlock) {
    return add(tsBlock, 0);
  }

  public LoadCursor add(TsBlock tsBlock, int startingPosition) {
    checkState(currentCursor == null, "Cursor still active");
    checkArgument(
        startingPosition >= 0 && startingPosition <= tsBlock.getPositionCount(),
        "invalid startingPosition: %s",
        startingPosition);

    TsBlockAccounting tsBlockAccounting =
        tsBlocks.allocateId(id -> new TsBlockAccounting(id, tsBlock));

    tsBlockAccounting.lockTsBlock();
    currentCursor =
        new LoadCursor(
            tsBlockAccounting,
            startingPosition,
            () -> {
              // Initiate additional actions on close
              checkState(currentCursor != null);
              tsBlockAccounting.unlockTsBlock();
              tsBlockAccounting.loadTsBlockLoadIfNeeded();
              // Account for tsBlock size after lazy loading (which can change the tsBlock size)
              tsBlockBytes += tsBlockAccounting.sizeOf();
              currentCursor = null;

              checkTsBlockMaintenance(tsBlockAccounting);
            });

    return currentCursor;
  }

  public void dereference(long rowId) {
    TsBlockAccounting tsBlockAccounting = tsBlocks.get(rowIdBuffer.getTsBlockId(rowId));
    tsBlockAccounting.dereference(rowId);
    checkTsBlockMaintenance(tsBlockAccounting);
  }

  private void checkTsBlockMaintenance(TsBlockAccounting tsBlockAccounting) {
    int tsBlockId = tsBlockAccounting.getTsBlockId();
    if (tsBlockAccounting.isPruneEligible()) {
      compactionCandidates.remove(tsBlockId);
      tsBlocks.deallocate(tsBlockId);
      tsBlockBytes -= tsBlockAccounting.sizeOf();
    } else if (tsBlockAccounting.isCompactionEligible()) {
      compactionCandidates.add(tsBlockId);
    }
  }

  public TsBlock getTsBlock(long rowId) {
    if (isCursorRowId(rowId)) {
      checkState(currentCursor != null, "No active cursor");
      return currentCursor.getTsBlock();
    }
    int tsBlockId = rowIdBuffer.getTsBlockId(rowId);
    return tsBlocks.get(tsBlockId).getTsBlock();
  }

  public int getPosition(long rowId) {
    if (isCursorRowId(rowId)) {
      checkState(currentCursor != null, "No active cursor");
      // rowId for cursors only reference the single current position
      return currentCursor.getCurrentPosition();
    }
    return rowIdBuffer.getPosition(rowId);
  }

  private static boolean isCursorRowId(long rowId) {
    return rowId == RESERVED_ROW_ID_FOR_CURSOR;
  }

  public void compactIfNeeded() {
    for (int tsBlockId : compactionCandidates) {
      TsBlockAccounting tsBlockAccounting = tsBlocks.get(tsBlockId);
      tsBlockBytes -= tsBlockAccounting.sizeOf();
      tsBlockAccounting.compact();
      tsBlockBytes += tsBlockAccounting.sizeOf();
    }
    compactionCandidates.clear();
  }

  public long sizeOf() {
    return INSTANCE_SIZE
        + tsBlockBytes
        + tsBlocks.sizeOf()
        + rowIdBuffer.sizeOf()
        + RamUsageEstimator.sizeOfHashSet(compactionCandidates);
  }

  /**
   * Cursor that allows callers to advance through the registered page and dictate whether a
   * specific position should be preserved with a stable row ID. Row ID generation can be expensive
   * in tight loops, so this allows callers to quickly skip positions that won't be needed.
   */
  public static final class LoadCursor implements RowReference, AutoCloseable {
    private final TsBlockAccounting tsBlockAccounting;
    private final Runnable closeCallback;

    private int currentPosition;

    private LoadCursor(
        TsBlockAccounting tsBlockAccounting, int startingPosition, Runnable closeCallback) {
      this.tsBlockAccounting = tsBlockAccounting;
      this.currentPosition = startingPosition - 1;
      this.closeCallback = closeCallback;
    }

    private TsBlock getTsBlock() {
      return tsBlockAccounting.getTsBlock();
    }

    private int getCurrentPosition() {
      checkState(currentPosition >= 0, "Not yet advanced");
      return currentPosition;
    }

    public boolean advance() {
      if (currentPosition >= tsBlockAccounting.getTsBlock().getPositionCount() - 1) {
        return false;
      }
      currentPosition++;
      return true;
    }

    @Override
    public int compareTo(RowIdComparisonStrategy strategy, long rowId) {
      checkState(currentPosition >= 0, "Not yet advanced");
      return strategy.compare(RESERVED_ROW_ID_FOR_CURSOR, rowId);
    }

    @Override
    public boolean equals(RowIdHashStrategy strategy, long rowId) {
      checkState(currentPosition >= 0, "Not yet advanced");
      return strategy.equals(RESERVED_ROW_ID_FOR_CURSOR, rowId);
    }

    @Override
    public long hash(RowIdHashStrategy strategy) {
      checkState(currentPosition >= 0, "Not yet advanced");
      return strategy.hashCode(RESERVED_ROW_ID_FOR_CURSOR);
    }

    @Override
    public long allocateRowId() {
      checkState(currentPosition >= 0, "Not yet advanced");
      return tsBlockAccounting.referencePosition(currentPosition);
    }

    @Override
    public void close() {
      closeCallback.run();
    }
  }

  private final class TsBlockAccounting {
    private static final int COMPACTION_MIN_FILL_MULTIPLIER = 2;

    private final int tsBlockId;
    private TsBlock tsBlock;
    private boolean isTsBlockLoaded;
    private long[] rowIds;
    // Start off locked to give the caller time to declare which rows to reference
    private boolean lockedTsBlock = true;
    private int activePositions;

    public TsBlockAccounting(int tsBlockId, TsBlock tsBlock) {
      this.tsBlockId = tsBlockId;
      this.tsBlock = tsBlock;
      rowIds = new long[tsBlock.getPositionCount()];
      Arrays.fill(rowIds, RowIdBuffer.UNKNOWN_ID);
    }

    /** Record the position as referenced and return a corresponding stable row ID */
    public long referencePosition(int position) {
      long rowId = rowIds[position];
      if (rowId == RowIdBuffer.UNKNOWN_ID) {
        rowId = rowIdBuffer.allocateRowId(tsBlockId, position);
        rowIds[position] = rowId;
        activePositions++;
      }
      return rowId;
    }

    /**
     * Locks the current TsBlock so that it can't be compacted (thus allowing for stable
     * position-based access).
     */
    public void lockTsBlock() {
      lockedTsBlock = true;
    }

    /** Unlocks the current TsBlock so that it becomes eligible for compaction. */
    public void unlockTsBlock() {
      lockedTsBlock = false;
    }

    public int getTsBlockId() {
      return tsBlockId;
    }

    public TsBlock getTsBlock() {
      return tsBlock;
    }

    /** Dereferences the row ID from this TsBlock. */
    public void dereference(long rowId) {
      int position = rowIdBuffer.getPosition(rowId);
      checkArgument(rowId == rowIds[position], "rowId does not match this TsBlock");
      rowIds[position] = RowIdBuffer.UNKNOWN_ID;
      activePositions--;
      rowIdBuffer.deallocate(rowId);
    }

    public boolean isPruneEligible() {
      // Pruning is only allowed if the TsBlock is unlocked
      return !lockedTsBlock && activePositions == 0;
    }

    public boolean isCompactionEligible() {
      // Compaction is only allowed if the TsBlock is unlocked
      return !lockedTsBlock
          && activePositions * COMPACTION_MIN_FILL_MULTIPLIER < tsBlock.getPositionCount();
    }

    public void loadTsBlockLoadIfNeeded() {
      if (!isTsBlockLoaded && activePositions > 0) {
        //        tsBlock = tsBlock.getLoadedPage();
        isTsBlockLoaded = true;
      }
    }

    public void compact() {
      checkState(!lockedTsBlock, "Should not attempt compaction when TsBlock is locked");

      if (activePositions == tsBlock.getPositionCount()) {
        return;
      }

      loadTsBlockLoadIfNeeded();

      int newIndex = 0;
      int[] positionsToKeep = new int[activePositions];
      long[] newRowIds = new long[activePositions];
      for (int i = 0; i < tsBlock.getPositionCount() && newIndex < positionsToKeep.length; i++) {
        long rowId = rowIds[i];
        positionsToKeep[newIndex] = i;
        newRowIds[newIndex] = rowId;
        newIndex += rowId == RowIdBuffer.UNKNOWN_ID ? 0 : 1;
      }
      verify(newIndex == activePositions);
      for (int i = 0; i < newRowIds.length; i++) {
        rowIdBuffer.setPosition(newRowIds[i], i);
      }

      // Compact TsBlock
      //      page = page.copyPositions(positionsToKeep, 0, positionsToKeep.length);
      rowIds = newRowIds;
    }

    public long sizeOf() {
      // Getting the size of a TsBlock forces a lazy TsBlock to be loaded, so only provide the size
      // after
      // an explicit decision to load
      long loadedTsBlockSize = isTsBlockLoaded ? tsBlock.getSizeInBytes() : 0;
      return TSBLOCK_ACCOUNTING_INSTANCE_SIZE
          + loadedTsBlockSize
          + RamUsageEstimator.sizeOf(rowIds);
    }
  }

  /**
   * Buffer abstracting a mapping between row IDs and their associated TsBlock IDs and positions.
   */
  private static class RowIdBuffer {
    public static final long UNKNOWN_ID = -1;
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(RowIdBuffer.class);

    /*
     *  Memory layout:
     *  [INT] TsBlockId1, [INT] position1,
     *  [INT] TsBlockId2, [INT] position2,
     *  ...
     */
    private final IntBigArray buffer = new IntBigArray();

    private final LongBigArrayFIFOQueue emptySlots = new LongBigArrayFIFOQueue();

    private long capacity;

    /**
     * Provides a new row ID referencing the provided TsBlock position.
     *
     * @return ID referencing the provided TsBlock position
     */
    public long allocateRowId(int tsBlockId, int position) {
      long newRowId;
      if (!emptySlots.isEmpty()) {
        newRowId = emptySlots.dequeueLong();
      } else {
        newRowId = capacity;
        capacity++;
        buffer.ensureCapacity(capacity * 2);
      }

      setTsBlockId(newRowId, tsBlockId);
      setPosition(newRowId, position);

      return newRowId;
    }

    public void deallocate(long rowId) {
      emptySlots.enqueue(rowId);
    }

    public int getTsBlockId(long rowId) {
      return buffer.get(rowId * 2);
    }

    public void setTsBlockId(long rowId, int tsBlockId) {
      buffer.set(rowId * 2, tsBlockId);
    }

    public int getPosition(long rowId) {
      return buffer.get(rowId * 2 + 1);
    }

    public void setPosition(long rowId, int position) {
      buffer.set(rowId * 2 + 1, position);
    }

    public long sizeOf() {
      return INSTANCE_SIZE + buffer.sizeOf() + emptySlots.sizeOf();
    }
  }
}
