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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.addExact;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.VariableWidthData.EMPTY_CHUNK;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.VariableWidthData.POINTER_SIZE;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOf;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfByteArray;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfIntArray;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfObjectArray;

public final class FlatHash {
  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(FlatHash.class);

  private static final double DEFAULT_LOAD_FACTOR = 15.0 / 16;

  private static int computeCapacity(int maxSize, double loadFactor) {
    int capacity = (int) (maxSize / loadFactor);
    return max(toIntExact(1L << (64 - Long.numberOfLeadingZeros(capacity - 1))), 16);
  }

  private static final int RECORDS_PER_GROUP_SHIFT = 10;
  private static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
  private static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;

  private static final int VECTOR_LENGTH = Long.BYTES;

  private final FlatHashStrategy flatHashStrategy;
  private final boolean hasPrecomputedHash;

  private final int recordSize;
  private final int recordGroupIdOffset;
  private final int recordHashOffset;
  private final int recordValueOffset;

  private int capacity;
  private int mask;

  private byte[] control;
  private byte[][] recordGroups;
  private final VariableWidthData variableWidthData;

  // position of each group in the hash table
  private int[] groupRecordIndex;

  // reserve enough memory before rehash
  private final UpdateMemory checkMemoryReservation;
  private long fixedSizeEstimate;
  private long rehashMemoryReservation;

  private int nextGroupId;
  private int maxFill;

  public FlatHash(
      FlatHashStrategy flatHashStrategy,
      boolean hasPrecomputedHash,
      int expectedSize,
      UpdateMemory checkMemoryReservation) {
    this.flatHashStrategy = flatHashStrategy;
    this.hasPrecomputedHash = hasPrecomputedHash;
    this.checkMemoryReservation = checkMemoryReservation;

    capacity = max(VECTOR_LENGTH, computeCapacity(expectedSize, DEFAULT_LOAD_FACTOR));
    maxFill = calculateMaxFill(capacity);
    mask = capacity - 1;
    control = new byte[capacity + VECTOR_LENGTH];

    groupRecordIndex = new int[maxFill];

    // the record is laid out as follows:
    // 1. optional variable width pointer
    // 2. groupId (int)
    // 3. fixed data for each type
    boolean variableWidth = flatHashStrategy.isAnyVariableWidth();
    variableWidthData = variableWidth ? new VariableWidthData() : null;
    recordGroupIdOffset = (variableWidth ? POINTER_SIZE : 0);
    recordHashOffset = recordGroupIdOffset + Integer.BYTES;
    recordValueOffset = recordHashOffset + (hasPrecomputedHash ? Long.BYTES : 0);
    recordSize = recordValueOffset + flatHashStrategy.getTotalFlatFixedLength();

    recordGroups = createRecordGroups(capacity, recordSize);
    fixedSizeEstimate = computeFixedSizeEstimate(capacity, recordSize);
  }

  public long getEstimatedSize() {
    return sumExact(
        fixedSizeEstimate,
        (variableWidthData == null ? 0 : variableWidthData.getRetainedSizeBytes()),
        rehashMemoryReservation);
  }

  public int size() {
    return nextGroupId;
  }

  public int getCapacity() {
    return capacity;
  }

  public static long bytesToLong(byte[] bytes, int index) {
    if (bytes.length - 8 < 4) {
      throw new IllegalArgumentException("Invalid input: bytes.length - offset < 8");
    }
    long num = 0;
    for (int ix = index + 7; ix >= index; ix--) {
      num <<= 8;
      num |= (bytes[ix] & 0xff);
    }
    return num;
  }

  public static int bytesToInt(byte[] bytes, int index) {
    if (bytes.length - index < 4) {
      throw new IllegalArgumentException("Invalid input: bytes.length - offset < 4");
    }

    int num = 0;
    for (int ix = index + 3; ix >= index; ix--) {
      num <<= 8;
      num |= (bytes[ix] & 0xff);
    }
    return num;
  }

  public static void intToBytes(byte[] desc, int offset, int i) {
    if (desc.length - offset < 4) {
      throw new IllegalArgumentException("Invalid input: desc.length - offset < 4");
    }
    desc[3 + offset] = (byte) ((i >> 24) & 0xFF);
    desc[2 + offset] = (byte) ((i >> 16) & 0xFF);
    desc[1 + offset] = (byte) ((i >> 8) & 0xFF);
    desc[offset] = (byte) (i & 0xFF);
  }

  private static void longToBytes(byte[] desc, int offset, long num) {
    if (desc.length - offset < 8) {
      throw new IllegalArgumentException("Invalid input: desc.length - offset < 4");
    }
    for (int ix = 0; ix < 8; ++ix) {
      int i = ix * 8;
      desc[ix + offset] = (byte) ((num >> i) & 0xff);
    }
  }

  public long hashPosition(int groupId) {
    // for spilling
    checkArgument(groupId < nextGroupId, "groupId out of range");

    int index = groupRecordIndex[groupId];
    byte[] records = getRecords(index);
    if (hasPrecomputedHash) {
      return bytesToLong(records, getRecordOffset(index) + recordHashOffset);
    } else {
      return valueHashCode(records, index);
    }
  }

  public void appendTo(int groupId, ColumnBuilder[] columnBuilders) {
    checkArgument(groupId < nextGroupId, "groupId out of range");
    int index = groupRecordIndex[groupId];
    byte[] records = getRecords(index);
    int recordOffset = getRecordOffset(index);

    byte[] variableWidthChunk = EMPTY_CHUNK;
    if (variableWidthData != null) {
      variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
    }

    flatHashStrategy.readFlat(
        records, recordOffset + recordValueOffset, variableWidthChunk, columnBuilders);
    if (hasPrecomputedHash) {
      columnBuilders[columnBuilders.length - 1].writeLong(
          bytesToLong(records, recordOffset + recordHashOffset));
    }
  }

  public boolean contains(Column[] columns, int position) {
    return contains(columns, position, flatHashStrategy.hash(columns, position));
  }

  public boolean contains(Column[] columns, int position, long hash) {
    return getIndex(columns, position, hash) >= 0;
  }

  public void computeHashes(Column[] columns, long[] hashes, int offset, int length) {
    if (hasPrecomputedHash) {
      Column hashColumn = columns[columns.length - 1];
      for (int i = 0; i < length; i++) {
        hashes[i] = hashColumn.getLong(offset + i);
      }
    } else {
      flatHashStrategy.hashBatched(columns, hashes, offset, length);
    }
  }

  public int putIfAbsent(Column[] columns, int position, long hash) {
    int index = getIndex(columns, position, hash);
    if (index >= 0) {
      return bytesToInt(getRecords(index), getRecordOffset(index) + recordGroupIdOffset);
    }

    index = -index - 1;
    int groupId = addNewGroup(index, columns, position, hash);
    if (nextGroupId >= maxFill) {
      rehash(0);
    }
    return groupId;
  }

  public int putIfAbsent(Column[] columns, int position) {
    long hash;
    /*if (hasPrecomputedHash) {
       hash = BIGINT.getLong(columns[columns.length - 1], position);
    } */

    hash = flatHashStrategy.hash(columns, position);

    return putIfAbsent(columns, position, hash);
  }

  private int getIndex(Column[] columns, int position, long hash) {
    byte hashPrefix = (byte) (hash & 0x7F | 0x80);
    int bucket = bucket((int) (hash >> 7));

    int step = 1;
    long repeated = repeat(hashPrefix);

    while (true) {
      final long controlVector = bytesToLong(control, bucket);

      int matchIndex = matchInVector(columns, position, hash, bucket, repeated, controlVector);
      if (matchIndex >= 0) {
        return matchIndex;
      }

      int emptyIndex = findEmptyInVector(controlVector, bucket);
      if (emptyIndex >= 0) {
        return -emptyIndex - 1;
      }

      bucket = bucket(bucket + step);
      step += VECTOR_LENGTH;
    }
  }

  private int matchInVector(
      Column[] columns,
      int position,
      long hash,
      int vectorStartBucket,
      long repeated,
      long controlVector) {
    long controlMatches = match(controlVector, repeated);
    while (controlMatches != 0) {
      int index = bucket(vectorStartBucket + (Long.numberOfTrailingZeros(controlMatches) >>> 3));
      if (valueNotDistinctFrom(index, columns, position, hash)) {
        return index;
      }

      controlMatches = controlMatches & (controlMatches - 1);
    }
    return -1;
  }

  private int findEmptyInVector(long vector, int vectorStartBucket) {
    long controlMatches = match(vector, 0x00_00_00_00_00_00_00_00L);
    if (controlMatches == 0) {
      return -1;
    }
    int slot = Long.numberOfTrailingZeros(controlMatches) >>> 3;
    return bucket(vectorStartBucket + slot);
  }

  private int addNewGroup(int index, Column[] columns, int position, long hash) {
    setControl(index, (byte) (hash & 0x7F | 0x80));

    byte[] records = getRecords(index);
    int recordOffset = getRecordOffset(index);

    int groupId = nextGroupId++;
    intToBytes(records, recordOffset + recordGroupIdOffset, groupId);
    groupRecordIndex[groupId] = index;

    if (hasPrecomputedHash) {
      longToBytes(records, recordOffset + recordHashOffset, hash);
    }

    byte[] variableWidthChunk = EMPTY_CHUNK;
    int variableWidthChunkOffset = 0;
    if (variableWidthData != null) {
      int variableWidthSize = flatHashStrategy.getTotalVariableWidth(columns, position);
      variableWidthChunk = variableWidthData.allocate(records, recordOffset, variableWidthSize);
      variableWidthChunkOffset = VariableWidthData.getChunkOffset(records, recordOffset);
    }
    flatHashStrategy.writeFlat(
        columns,
        position,
        records,
        recordOffset + recordValueOffset,
        variableWidthChunk,
        variableWidthChunkOffset);
    return groupId;
  }

  private void setControl(int index, byte hashPrefix) {
    control[index] = hashPrefix;
    if (index < VECTOR_LENGTH) {
      control[index + capacity] = hashPrefix;
    }
  }

  public boolean ensureAvailableCapacity(int batchSize) {
    long requiredMaxFill = nextGroupId + batchSize;
    if (requiredMaxFill >= maxFill) {
      long minimumRequiredCapacity = (requiredMaxFill + 1) * 16 / 15;
      return tryRehash(toIntExact(minimumRequiredCapacity));
    }
    return true;
  }

  private boolean tryRehash(int minimumRequiredCapacity) {
    int newCapacity = computeNewCapacity(minimumRequiredCapacity);

    // update the fixed size estimate to the new size as we will need this much memory after the
    // rehash
    fixedSizeEstimate = computeFixedSizeEstimate(newCapacity, recordSize);

    // the rehash incrementally allocates the new records as needed, so as new memory is added old
    // memory is released
    // while the rehash is in progress, the old control array is retained, and one additional record
    // group is retained
    rehashMemoryReservation = sumExact(sizeOf(control), sizeOf(recordGroups[0]));
    verify(rehashMemoryReservation >= 0, "rehashMemoryReservation is negative");
    if (!checkMemoryReservation.update()) {
      return false;
    }

    rehash(minimumRequiredCapacity);
    return true;
  }

  private void rehash(int minimumRequiredCapacity) {
    int oldCapacity = capacity;
    byte[] oldControl = control;
    byte[][] oldRecordGroups = recordGroups;

    capacity = computeNewCapacity(minimumRequiredCapacity);
    maxFill = calculateMaxFill(capacity);
    mask = capacity - 1;

    control = new byte[capacity + VECTOR_LENGTH];

    // we incrementally allocate the record groups to smooth out memory allocation
    if (capacity <= RECORDS_PER_GROUP) {
      recordGroups = new byte[][] {new byte[multiplyExact(capacity, recordSize)]};
    } else {
      recordGroups = new byte[(capacity + 1) >> RECORDS_PER_GROUP_SHIFT][];
    }

    groupRecordIndex = new int[maxFill];

    for (int oldRecordGroupIndex = 0;
        oldRecordGroupIndex < oldRecordGroups.length;
        oldRecordGroupIndex++) {
      byte[] oldRecords = oldRecordGroups[oldRecordGroupIndex];
      oldRecordGroups[oldRecordGroupIndex] = null;
      for (int indexInRecordGroup = 0;
          indexInRecordGroup < min(RECORDS_PER_GROUP, oldCapacity);
          indexInRecordGroup++) {
        int oldIndex = (oldRecordGroupIndex << RECORDS_PER_GROUP_SHIFT) + indexInRecordGroup;
        if (oldControl[oldIndex] == 0) {
          continue;
        }

        long hash;
        if (hasPrecomputedHash) {
          hash = bytesToLong(oldRecords, getRecordOffset(oldIndex) + recordHashOffset);
        } else {
          hash = valueHashCode(oldRecords, oldIndex);
        }

        byte hashPrefix = (byte) (hash & 0x7F | 0x80);
        int bucket = bucket((int) (hash >> 7));

        // getIndex is not used here because values in a rehash are always distinct
        int step = 1;
        while (true) {
          final long controlVector = bytesToLong(control, bucket);
          // values are already distinct, so just find the first empty slot
          int emptyIndex = findEmptyInVector(controlVector, bucket);
          if (emptyIndex >= 0) {
            setControl(emptyIndex, hashPrefix);

            int newRecordGroupIndex = emptyIndex >> RECORDS_PER_GROUP_SHIFT;
            byte[] records = recordGroups[newRecordGroupIndex];
            if (records == null) {
              records = new byte[multiplyExact(RECORDS_PER_GROUP, recordSize)];
              recordGroups[newRecordGroupIndex] = records;
            }
            int recordOffset = getRecordOffset(emptyIndex);
            int oldRecordOffset = getRecordOffset(oldIndex);
            System.arraycopy(oldRecords, oldRecordOffset, records, recordOffset, recordSize);

            int groupId = bytesToInt(records, recordOffset + recordGroupIdOffset);
            groupRecordIndex[groupId] = emptyIndex;

            break;
          }

          bucket = bucket(bucket + step);
          step += VECTOR_LENGTH;
        }
      }
    }

    // add any completely empty record groups
    // the odds of needing this are exceedingly low, but it is technically possible
    for (int i = 0; i < recordGroups.length; i++) {
      if (recordGroups[i] == null) {
        recordGroups[i] = new byte[multiplyExact(RECORDS_PER_GROUP, recordSize)];
      }
    }

    // release temporary memory reservation
    rehashMemoryReservation = 0;
    fixedSizeEstimate = computeFixedSizeEstimate(capacity, recordSize);
    checkMemoryReservation.update();
  }

  private int computeNewCapacity(int minimumRequiredCapacity) {
    checkArgument(minimumRequiredCapacity >= 0, "minimumRequiredCapacity must be positive");
    long newCapacityLong = capacity * 2L;
    while (newCapacityLong < minimumRequiredCapacity) {
      newCapacityLong = multiplyExact(newCapacityLong, 2);
    }
    if (newCapacityLong > Integer.MAX_VALUE) {
      throw new RuntimeException("Size of hash table cannot exceed 1 billion entries");
    }
    return toIntExact(newCapacityLong);
  }

  // copy from JDK
  public static long multiplyExact(long x, long y) {
    long r = x * y;
    long ax = Math.abs(x);
    long ay = Math.abs(y);
    if (((ax | ay) >>> 31 != 0)) {
      // Some bits greater than 2^31 that might cause overflow
      // Check the result using the divide operator
      // and check for the special case of Long.MIN_VALUE * -1
      if (((y != 0) && (r / y != x)) || (x == Long.MIN_VALUE && y == -1)) {
        throw new ArithmeticException("long overflow");
      }
    }
    return r;
  }

  // copy from JDK
  public static int multiplyExact(int x, int y) {
    long r = (long) x * (long) y;
    if ((int) r != r) {
      throw new ArithmeticException("integer overflow");
    }
    return (int) r;
  }

  private int bucket(int hash) {
    return hash & mask;
  }

  private byte[] getRecords(int index) {
    return recordGroups[index >> RECORDS_PER_GROUP_SHIFT];
  }

  private int getRecordOffset(int index) {
    return (index & RECORDS_PER_GROUP_MASK) * recordSize;
  }

  private long valueHashCode(byte[] records, int index) {
    int recordOffset = getRecordOffset(index);

    try {
      byte[] variableWidthChunk = EMPTY_CHUNK;
      if (variableWidthData != null) {
        variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
      }

      return flatHashStrategy.hash(records, recordOffset + recordValueOffset, variableWidthChunk);
    } catch (Throwable throwable) {
      throwIfUnchecked(throwable);
      throw new RuntimeException(throwable);
    }
  }

  private boolean valueNotDistinctFrom(
      int leftIndex, Column[] rightColumns, int rightPosition, long rightHash) {
    byte[] leftRecords = getRecords(leftIndex);
    int leftRecordOffset = getRecordOffset(leftIndex);

    if (hasPrecomputedHash) {
      long leftHash = bytesToLong(leftRecords, leftRecordOffset + recordHashOffset);
      if (leftHash != rightHash) {
        return false;
      }
    }

    byte[] leftVariableWidthChunk = EMPTY_CHUNK;
    if (variableWidthData != null) {
      leftVariableWidthChunk = variableWidthData.getChunk(leftRecords, leftRecordOffset);
    }

    return flatHashStrategy.valueNotDistinctFrom(
        leftRecords,
        leftRecordOffset + recordValueOffset,
        leftVariableWidthChunk,
        rightColumns,
        rightPosition);
  }

  private static long repeat(byte value) {
    return ((value & 0xFF) * 0x01_01_01_01_01_01_01_01L);
  }

  private static long match(long vector, long repeatedValue) {
    // HD 6-1
    long comparison = vector ^ repeatedValue;
    return (comparison - 0x01_01_01_01_01_01_01_01L) & ~comparison & 0x80_80_80_80_80_80_80_80L;
  }

  public int getPhysicalPosition(int groupId) {
    return groupRecordIndex[groupId];
  }

  private static int calculateMaxFill(int capacity) {
    return toIntExact(capacity * 15L / 16);
  }

  private static byte[][] createRecordGroups(int capacity, int recordSize) {
    if (capacity <= RECORDS_PER_GROUP) {
      return new byte[][] {new byte[multiplyExact(capacity, recordSize)]};
    }

    byte[][] groups = new byte[(capacity + 1) >> RECORDS_PER_GROUP_SHIFT][];
    for (int i = 0; i < groups.length; i++) {
      groups[i] = new byte[multiplyExact(RECORDS_PER_GROUP, recordSize)];
    }
    return groups;
  }

  private static long computeRecordGroupsSize(int capacity, int recordSize) {
    if (capacity <= RECORDS_PER_GROUP) {
      return sizeOfObjectArray(1) + sizeOfByteArray(multiplyExact(capacity, recordSize));
    }

    int groupCount = addExact(capacity, 1) >> RECORDS_PER_GROUP_SHIFT;
    return sizeOfObjectArray(groupCount)
        + multiplyExact(groupCount, sizeOfByteArray(multiplyExact(RECORDS_PER_GROUP, recordSize)));
  }

  private static long computeFixedSizeEstimate(int capacity, int recordSize) {
    return sumExact(
        INSTANCE_SIZE,
        sizeOfByteArray(capacity + VECTOR_LENGTH),
        computeRecordGroupsSize(capacity, recordSize),
        sizeOfIntArray(capacity));
  }

  public static long sumExact(long... values) {
    long result = 0;
    for (long value : values) {
      result = addExact(result, value);
    }
    return result;
  }
}
