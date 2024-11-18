/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory;
import org.apache.iotdb.db.queryengine.plan.relational.utils.TypeUtil;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.min;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.FlatHash.sumExact;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOf;

// This implementation assumes arrays used in the hash are always a power of 2
public class FlatGroupByHash implements GroupByHash {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FlatGroupByHash.class);
  private static final int BATCH_SIZE = 1024;
  // Max (page value count / cumulative dictionary size) to trigger the low cardinality case
  private static final double SMALL_DICTIONARIES_MAX_CARDINALITY_RATIO = 0.25;

  private final FlatHash flatHash;
  private final int groupByChannelCount;
  private final boolean hasPrecomputedHash;

  private long currentPageSizeInBytes;

  private final Column[] currentColumns;
  private final ColumnBuilder[] currentColumnBuilders;
  // reusable array for computing hash batches into
  private long[] currentHashes;

  public FlatGroupByHash(
      List<Type> hashTypes,
      boolean hasPrecomputedHash,
      int expectedSize,
      UpdateMemory checkMemoryReservation) {
    this.flatHash =
        new FlatHash(
            TypeUtil.getFlatHashStrategy(hashTypes),
            hasPrecomputedHash,
            expectedSize,
            checkMemoryReservation);
    this.groupByChannelCount = hashTypes.size();
    this.hasPrecomputedHash = hasPrecomputedHash;

    checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

    int totalChannels = hashTypes.size() + (hasPrecomputedHash ? 1 : 0);
    this.currentColumns = new Column[totalChannels];
    this.currentColumnBuilders = new ColumnBuilder[totalChannels];
  }

  public int getPhysicalPosition(int groupId) {
    return flatHash.getPhysicalPosition(groupId);
  }

  @Override
  public long getRawHash(int groupId) {
    return flatHash.hashPosition(groupId);
  }

  @Override
  public long getEstimatedSize() {
    return sumExact(
        INSTANCE_SIZE, flatHash.getEstimatedSize(), currentPageSizeInBytes, sizeOf(currentHashes));
  }

  @Override
  public int getGroupCount() {
    return flatHash.size();
  }

  @Override
  public void appendValuesTo(int groupId, TsBlockBuilder pageBuilder) {
    ColumnBuilder[] columnBuilders = currentColumnBuilders;
    for (int i = 0; i < columnBuilders.length; i++) {
      columnBuilders[i] = pageBuilder.getValueColumnBuilders()[i];
    }
    flatHash.appendTo(groupId, columnBuilders);
  }

  @Override
  public void addPage(Column[] page) {
    if (page[0].getPositionCount() == 0) {
      return;
    }

    currentPageSizeInBytes = Arrays.stream(page).mapToLong(Column::getRetainedSizeInBytes).sum();

    Column[] columns = getColumnsFromPage(page);
    addNonDictionaryPageWork(columns);
  }

  @Override
  public int[] getGroupIds(Column[] page) {
    if (page[0].getPositionCount() == 0) {
      return new int[0];
    }

    currentPageSizeInBytes = Arrays.stream(page).mapToLong(Column::getRetainedSizeInBytes).sum();
    Column[] columns = getColumnsFromPage(page);

    return getNonDictionaryPageWork(columns);
  }

  @Override
  public int getCapacity() {
    return flatHash.getCapacity();
  }

  private int putIfAbsent(Column[] columns, int position) {
    return flatHash.putIfAbsent(columns, position);
  }

  private long[] getHashesBufferArray() {
    if (currentHashes == null) {
      currentHashes = new long[BATCH_SIZE];
    }
    return currentHashes;
  }

  private Column[] getColumnsFromPage(Column[] page) {
    Column[] blocks = currentColumns;
    checkArgument(page.length == blocks.length);
    System.arraycopy(page, 0, blocks, 0, blocks.length);
    return blocks;
  }

  public void addNonDictionaryPageWork(Column[] columns) {
    int lastPosition = 0;

    int positionCount = columns[0].getPositionCount();
    checkState(lastPosition <= positionCount, "position count out of bound");

    int remainingPositions = positionCount - lastPosition;

    long[] hashes = getHashesBufferArray();
    while (remainingPositions != 0) {
      int batchSize = min(remainingPositions, hashes.length);
      if (!flatHash.ensureAvailableCapacity(batchSize)) {
        throw new RuntimeException("Memory for flatHash is not enough");
      }

      flatHash.computeHashes(columns, hashes, lastPosition, batchSize);
      for (int i = 0; i < batchSize; i++) {
        flatHash.putIfAbsent(columns, lastPosition + i, hashes[i]);
      }

      lastPosition += batchSize;
      remainingPositions -= batchSize;
    }
    verify(lastPosition == positionCount);
  }

  public int[] getNonDictionaryPageWork(Column[] columns) {
    int lastPosition = 0;

    int positionCount = columns[0].getPositionCount();
    int[] groupIds = new int[positionCount];
    checkState(lastPosition <= positionCount, "position count out of bound");

    int remainingPositions = positionCount - lastPosition;

    long[] hashes = getHashesBufferArray();
    while (remainingPositions != 0) {
      int batchSize = min(remainingPositions, hashes.length);
      if (!flatHash.ensureAvailableCapacity(batchSize)) {
        throw new RuntimeException("Memory for flatHash is not enough");
      }

      flatHash.computeHashes(columns, hashes, lastPosition, batchSize);
      for (int i = 0, position = lastPosition; i < batchSize; i++, position++) {
        groupIds[position] = flatHash.putIfAbsent(columns, lastPosition + i, hashes[i]);
      }

      lastPosition += batchSize;
      remainingPositions -= batchSize;
    }
    verify(lastPosition == positionCount);
    return groupIds;
  }
}
