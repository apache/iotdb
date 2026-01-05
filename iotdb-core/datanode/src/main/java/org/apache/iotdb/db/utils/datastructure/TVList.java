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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.MathUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;
import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

public abstract class TVList implements WALEntryValue {
  protected static final String ERR_DATATYPE_NOT_CONSISTENT = "DataType not consistent";
  // list of timestamp array, add 1 when expanded -> data point timestamp array
  // index relation: arrayIndex -> elementIndex
  protected List<long[]> timestamps;
  protected int rowCount;
  // the count of sequential part started from the beginning
  protected int seqRowCount;

  // List of index array, add 1 when expanded -> data point index array
  // Index relation: arrayIndex -> elementIndex
  // Used in sort method, sort only changes indices
  protected List<int[]> indices;

  // used by non-aligned TVList
  // Index relation: arrayIndex -> elementIndex
  protected List<BitMap> bitMap;

  // Guards queryContextSet, ownerQuery, and reservedMemoryBytes.
  // Always acquire this lock before accessing/modifying these fields.
  private final ReentrantLock queryListLock = new ReentrantLock();

  // set of query that this TVList is used
  protected final Set<QueryContext> queryContextSet;

  // the owner query which is obligated to release the TVList.
  // When it is null, the TVList is owned by insert thread and released after flush.
  protected QueryContext ownerQuery;

  // Reserved memory by the query. Ensure to acquire queryListLock before update.
  protected long reservedMemoryBytes = 0L;

  protected boolean sorted = true;
  protected long maxTime;
  protected long minTime;
  // record reference count of this tv list
  // currently this reference will only be increase because we can't know when to decrease it
  protected AtomicInteger referenceCount;
  private long version;

  private final TVList outer = this;

  protected static int defaultArrayNum = 0;
  protected static volatile long defaultArrayNumLastUpdatedTimeMs = 0;

  protected TSDataType dataType;

  protected TVList() {
    timestamps = new ArrayList<>(getDefaultArrayNum());
    rowCount = 0;
    seqRowCount = 0;
    maxTime = Long.MIN_VALUE;
    minTime = Long.MAX_VALUE;
    queryContextSet = new HashSet<>();
    referenceCount = new AtomicInteger();
  }

  public static TVList newList(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
      case BLOB:
      case STRING:
      case OBJECT:
        return BinaryTVList.newList();
      case FLOAT:
        return FloatTVList.newList();
      case INT32:
        return IntTVList.newList(TSDataType.INT32);
      case DATE:
        return IntTVList.newList(TSDataType.DATE);
      case INT64:
      case TIMESTAMP:
        return LongTVList.newList();
      case DOUBLE:
        return DoubleTVList.newList();
      case BOOLEAN:
        return BooleanTVList.newList();
      default:
        break;
    }
    return null;
  }

  // get array memory cost of working TVList
  public long tvListArrayMemCost() {
    long size = tvListArrayMemCost(getDataType());
    // index array mem size
    size += indices != null ? PrimitiveArrayManager.ARRAY_SIZE * 4L : 0;
    // bimap array mem size
    size += bitMap != null ? PrimitiveArrayManager.ARRAY_SIZE / 8 + 1L : 0;
    return size;
  }

  public static long tvListArrayMemCost(TSDataType type) {
    long size = 0;
    // time array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * 8L;
    // value array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * (long) type.getDataTypeSize();
    // two array headers mem size
    size += NUM_BYTES_ARRAY_HEADER * 2L;
    // Object references size in ArrayList
    size += NUM_BYTES_OBJECT_REF * 2L;
    return size;
  }

  public synchronized long calculateRamSize() {
    return timestamps.size() * tvListArrayMemCost();
  }

  public synchronized boolean isSorted() {
    return sorted;
  }

  public void setReservedMemoryBytes(long bytes) {
    this.reservedMemoryBytes = bytes;
  }

  public void addReservedMemoryBytes(long bytes) {
    this.reservedMemoryBytes += bytes;
  }

  public long getReservedMemoryBytes() {
    return reservedMemoryBytes;
  }

  public abstract void sort();

  public void increaseReferenceCount() {
    referenceCount.incrementAndGet();
  }

  public int getReferenceCount() {
    return referenceCount.get();
  }

  public int rowCount() {
    return rowCount;
  }

  public int seqRowCount() {
    return seqRowCount;
  }

  public int count() {
    if (bitMap == null) {
      return rowCount;
    }
    int count = 0;
    for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
      if (!isNullValue(rowIdx)) {
        count++;
      }
    }
    return count;
  }

  public long getTime(int index) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return timestamps.get(arrayIndex)[elementIndex];
  }

  /**
   * Performs a binary search to find the first position whose timestamp is greater than or equal to
   * the given {@code time}.
   *
   * <p>This method assumes timestamps are sorted in ascending order. If the list is not sorted, an
   * {@link UnsupportedOperationException} will be thrown.
   *
   * <p>Typical use case: locate the starting index of a time range query.
   *
   * <p>Example:
   *
   * <ul>
   *   <li>timestamps = [10, 20, 20, 25, 30]
   *   <li>time = 5 → return 0
   *   <li>time = 20 → return 1
   *   <li>time = 21 → return 3
   *   <li>time = 40 → return 5 (all timestamps &lt; 40)
   * </ul>
   *
   * <p><b>Return value range:</b>
   *
   * <ul>
   *   <li>When a matching or greater element exists: {@code 0 <= index <= seqRowCount - 1}
   *   <li>When all elements are smaller than {@code time}: {@code index == seqRowCount}
   * </ul>
   *
   * @param time the target timestamp
   * @param low the lower bound index (inclusive)
   * @param high the upper bound index (inclusive)
   * @return the index of the first timestamp ≥ {@code time}, or {@code seqRowCount} if all
   *     timestamps are smaller
   */
  private int binarySearchTimestampFirstGreaterOrEqualsPosition(long time, int low, int high) {
    if (!sorted && high >= seqRowCount) {
      throw new UnsupportedOperationException("Current TVList is not sorted");
    }
    int mid;
    while (low <= high) {
      mid = low + ((high - low) >>> 1);
      long midTime = getTime(mid);
      if (midTime < time) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return low;
  }

  /**
   * Performs a binary search to find the last position whose timestamp is less than or equal to the
   * given {@code time}.
   *
   * <p>This method assumes timestamps are sorted in ascending order. If the list is not sorted, an
   * {@link UnsupportedOperationException} will be thrown.
   *
   * <p>Typical use case: locate the ending index of a time range query.
   *
   * <p>Example:
   *
   * <ul>
   *   <li>timestamps = [10, 20, 20, 25, 30]
   *   <li>time = 5 → return -1 (no timestamp ≤ 5)
   *   <li>time = 20 → return 2
   *   <li>time = 21 → return 2
   *   <li>time = 50 → return 4
   * </ul>
   *
   * <p><b>Return value range:</b>
   *
   * <ul>
   *   <li>When a matching or smaller element exists: {@code 0 <= index <= seqRowCount - 1}
   *   <li>When all elements are greater than {@code time}: {@code index == -1}
   * </ul>
   *
   * @param time the target timestamp
   * @param low the lower bound index (inclusive)
   * @param high the upper bound index (inclusive)
   * @return the index of the last timestamp ≤ {@code time}, or {@code -1} if all timestamps are
   *     greater
   */
  private int binarySearchTimestampLastLessOrEqualsPosition(long time, int low, int high) {
    if (!sorted && high >= seqRowCount) {
      throw new UnsupportedOperationException("Current TVList is not sorted");
    }

    int mid;
    while (low <= high) {
      mid = low + ((high - low) >>> 1);
      long midTime = getTime(mid);
      if (midTime <= time) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return high;
  }

  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getValueIndex(src);
    set(dest, srcT, srcV);
  }

  protected void set(int index, long timestamp, int valueIndex) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    // prepare indices for sorting
    if (indices == null) {
      indices = new ArrayList<>(getDefaultArrayNum());
      for (int i = 0; i < timestamps.size(); i++) {
        indices.add((int[]) getPrimitiveArraysByType(TSDataType.INT32));
        int offset = i * ARRAY_SIZE;
        Arrays.setAll(indices.get(i), j -> offset + j);
      }
    }
    indices.get(arrayIndex)[elementIndex] = valueIndex;
  }

  protected int[] cloneIndex(int[] array) {
    return Arrays.copyOf(array, array.length);
  }

  /**
   * Get the row index value in index column.
   *
   * @param index row index
   */
  public int getValueIndex(int index) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    if (indices == null) {
      return index;
    }

    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return indices.get(arrayIndex)[elementIndex];
  }

  protected void markNullValue(int arrayIndex, int elementIndex) {
    // init bitMap if doesn't have
    if (bitMap == null) {
      List<BitMap> localBitMap = new ArrayList<>(getDefaultArrayNum());
      for (int i = 0; i < timestamps.size(); i++) {
        localBitMap.add(new BitMap(ARRAY_SIZE));
      }
      bitMap = localBitMap;
    }
    // if the bitmap in arrayIndex is null, init the bitmap
    if (bitMap.get(arrayIndex) == null) {
      bitMap.set(arrayIndex, new BitMap(ARRAY_SIZE));
    }

    // mark the null value in the current bitmap
    bitMap.get(arrayIndex).mark(elementIndex);
  }

  /**
   * Get whether value is null at the given position in TvList.
   *
   * @param unsortedRowIndex value index
   * @return boolean
   */
  public boolean isNullValue(int unsortedRowIndex) {
    if (unsortedRowIndex >= rowCount) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    if (bitMap == null || bitMap.get(unsortedRowIndex / ARRAY_SIZE) == null) {
      return false;
    }
    int arrayIndex = unsortedRowIndex / ARRAY_SIZE;
    int elementIndex = unsortedRowIndex % ARRAY_SIZE;
    return bitMap.get(arrayIndex).isMarked(elementIndex);
  }

  protected void cloneBitMap(TVList cloneList) {
    if (bitMap != null) {
      cloneList.bitMap = new ArrayList<>(bitMap.size());
      for (BitMap bm : bitMap) {
        cloneList.bitMap.add(bm == null ? null : bm.clone());
      }
    }
  }

  public void putLong(long time, long value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putInt(long time, int value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putFloat(long time, float value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putDouble(long time, double value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBinary(long time, Binary value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBoolean(long time, boolean value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putAlignedValue(long time, Object[] value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putLongs(long[] time, long[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putInts(long[] time, int[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putFloats(long[] time, float[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putDoubles(long[] time, double[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBinaries(long[] time, Binary[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBooleans(long[] time, boolean[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putAlignedValues(
      long[] time, Object[] value, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public int getInt(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public float getFloat(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public double getDouble(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public Binary getBinary(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public boolean getBoolean(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public Object getAlignedValue(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public TVList getTvListByColumnIndex(
      List<Integer> columnIndexList, List<TSDataType> dataTypeList, boolean ignoreAllNullRows) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public long getMaxTime() {
    return maxTime;
  }

  public long getMinTime() {
    return minTime;
  }

  public long getVersion() {
    return version;
  }

  protected abstract void expandValues();

  @Override
  public abstract TVList clone();

  public TVList clone(long version) {
    this.version = version;
    return clone();
  }

  public int delete(long lowerBound, long upperBound) {
    int deletedNumber = 0;
    long maxTime = Long.MIN_VALUE;
    long minTime = Long.MAX_VALUE;
    for (int i = 0; i < rowCount; i++) {
      long time = getTime(i);
      if (time >= lowerBound && time <= upperBound) {
        int originRowIndex = getValueIndex(i);
        if (!isNullValue(originRowIndex)) {
          int arrayIndex = originRowIndex / ARRAY_SIZE;
          int elementIndex = originRowIndex % ARRAY_SIZE;
          markNullValue(arrayIndex, elementIndex);
          deletedNumber++;
        }
      } else {
        maxTime = Math.max(time, maxTime);
        minTime = Math.min(time, minTime);
      }
    }
    return deletedNumber;
  }

  // common clone for both TVList and AlignedTVList
  protected void cloneAs(TVList cloneList) {
    // clone timestamps
    for (long[] timestampArray : timestamps) {
      cloneList.timestamps.add(cloneTime(timestampArray));
    }
    // clone indices
    if (indices != null) {
      cloneList.indices = new ArrayList<>(indices.size());
      for (int[] indicesArray : indices) {
        cloneList.indices.add(cloneIndex(indicesArray));
      }
    }
    cloneList.rowCount = rowCount;
    cloneList.seqRowCount = seqRowCount;
    cloneList.sorted = sorted;
    cloneList.maxTime = maxTime;
    cloneList.minTime = minTime;
  }

  public void clear() {
    rowCount = 0;
    seqRowCount = 0;
    sorted = true;
    maxTime = Long.MIN_VALUE;
    minTime = Long.MAX_VALUE;
    queryContextSet.clear();
    ownerQuery = null;
    clearTime();
    clearValue();
    clearIndices();
    clearBitMap();
  }

  protected void clearTime() {
    if (timestamps != null) {
      for (long[] dataArray : timestamps) {
        PrimitiveArrayManager.release(dataArray);
      }
      timestamps.clear();
    }
  }

  protected abstract void clearValue();

  protected void clearIndices() {
    if (indices != null) {
      for (int[] dataArray : indices) {
        PrimitiveArrayManager.release(dataArray);
      }
      indices.clear();
    }
  }

  protected void clearBitMap() {
    if (bitMap != null) {
      bitMap.clear();
    }
  }

  protected void checkExpansion() {
    if ((rowCount % ARRAY_SIZE) == 0) {
      expandValues();
      timestamps.add((long[]) getPrimitiveArraysByType(TSDataType.INT64));
    }
  }

  protected Object getPrimitiveArraysByType(TSDataType dataType) {
    return PrimitiveArrayManager.allocate(dataType);
  }

  protected long[] cloneTime(long[] array) {
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  void updateMinMaxTimeAndSorted(long[] time, int start, int end) {
    int length = time.length;
    long inPutMinTime = Long.MAX_VALUE;
    boolean inputSorted = true;
    int inputSeqRowCount = 0;
    for (int i = start; i < end; i++) {
      inPutMinTime = Math.min(inPutMinTime, time[i]);
      maxTime = Math.max(maxTime, time[i]);
      if (inputSorted) {
        if (i < length - 1 && time[i] > time[i + 1]) {
          inputSorted = false;
        } else {
          inputSeqRowCount++;
        }
      }
    }
    minTime = Math.min(minTime, inPutMinTime);
    if (sorted && (rowCount == 0 || time[start] >= getTime(rowCount - 1))) {
      seqRowCount += inputSeqRowCount;
    }
    sorted = sorted && inputSorted && (rowCount == 0 || inPutMinTime >= getTime(rowCount - 1));
  }

  /** for log */
  public abstract TimeValuePair getTimeValuePair(int index);

  protected abstract TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding);

  @TestOnly
  public TsBlock buildTsBlock() {
    return buildTsBlock(0, TSEncoding.PLAIN, null);
  }

  public TsBlock buildTsBlock(
      int floatPrecision, TSEncoding encoding, List<TimeRange> deletionList) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(this.getDataType()));
    writeValidValuesIntoTsBlock(builder, floatPrecision, encoding, deletionList);
    return builder.build();
  }

  protected abstract void writeValidValuesIntoTsBlock(
      TsBlockBuilder builder,
      int floatPrecision,
      TSEncoding encoding,
      List<TimeRange> deletionList);

  protected float roundValueWithGivenPrecision(
      float value, int floatPrecision, TSEncoding encoding) {
    if (!Float.isNaN(value) && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
      return MathUtils.roundWithGivenPrecision(value, floatPrecision);
    }
    return value;
  }

  protected double roundValueWithGivenPrecision(
      double value, int floatPrecision, TSEncoding encoding) {
    if (!Double.isNaN(value) && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
      return MathUtils.roundWithGivenPrecision(value, floatPrecision);
    }
    return value;
  }

  public abstract TSDataType getDataType();

  public static TVList deserialize(DataInputStream stream) throws IOException {
    TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    switch (dataType) {
      case TEXT:
      case BLOB:
      case OBJECT:
      case STRING:
        return BinaryTVList.deserialize(stream);
      case FLOAT:
        return FloatTVList.deserialize(stream);
      case INT32:
        return IntTVList.deserialize(stream, TSDataType.INT32);
      case DATE:
        return IntTVList.deserialize(stream, TSDataType.DATE);
      case INT64:
      case TIMESTAMP:
        return LongTVList.deserialize(stream);
      case DOUBLE:
        return DoubleTVList.deserialize(stream);
      case BOOLEAN:
        return BooleanTVList.deserialize(stream);
      default:
        break;
    }
    return null;
  }

  public static TVList deserializeWithoutBitMap(DataInputStream stream) throws IOException {
    TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    switch (dataType) {
      case TEXT:
      case BLOB:
      case STRING:
      case OBJECT:
        return BinaryTVList.deserializeWithoutBitMap(stream);
      case FLOAT:
        return FloatTVList.deserializeWithoutBitMap(stream);
      case INT32:
        return IntTVList.deserializeWithoutBitMap(stream, TSDataType.INT32);
      case DATE:
        return IntTVList.deserializeWithoutBitMap(stream, TSDataType.DATE);
      case INT64:
      case TIMESTAMP:
        return LongTVList.deserializeWithoutBitMap(stream);
      case DOUBLE:
        return DoubleTVList.deserializeWithoutBitMap(stream);
      case BOOLEAN:
        return BooleanTVList.deserializeWithoutBitMap(stream);
      default:
        break;
    }
    return null;
  }

  public List<long[]> getTimestamps() {
    return timestamps;
  }

  public List<int[]> getIndices() {
    return indices;
  }

  public void setOwnerQuery(QueryContext queryCtx) {
    this.ownerQuery = queryCtx;
  }

  public QueryContext getOwnerQuery() {
    return ownerQuery;
  }

  public Set<QueryContext> getQueryContextSet() {
    return queryContextSet;
  }

  public List<BitMap> getBitMap() {
    return bitMap;
  }

  public void lockQueryList() {
    queryListLock.lock();
  }

  public void unlockQueryList() {
    queryListLock.unlock();
  }

  public TVListIterator iterator(
      Ordering scanOrder,
      int rowCount,
      Filter globalTimeFilter,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    return new TVListIterator(
        scanOrder,
        rowCount,
        globalTimeFilter,
        deletionList,
        floatPrecision,
        encoding,
        maxNumberOfPointsInPage);
  }

  /* TVList Iterator */
  public class TVListIterator extends MemPointIterator {
    protected int index;
    protected int rows;
    protected boolean probeNext;

    protected final Filter globalTimeFilter;
    private final List<TimeRange> deletionList;
    private final int[] deleteCursor;
    private final int floatPrecision;
    private final TSEncoding encoding;

    // used by nextBatch during query
    protected final int maxNumberOfPointsInPage;

    public TVListIterator(
        Ordering scanOrder,
        int rowCount,
        Filter globalTimeFilter,
        List<TimeRange> deletionList,
        Integer floatPrecision,
        TSEncoding encoding,
        int maxNumberOfPointsInPage) {
      super(scanOrder);
      this.globalTimeFilter = globalTimeFilter;
      this.deletionList = deletionList;
      this.floatPrecision = floatPrecision != null ? floatPrecision : 0;
      this.encoding = encoding;
      this.index = 0;
      this.rows = rowCount;
      this.probeNext = false;
      this.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
      int cursor =
          (deletionList == null || scanOrder.isAscending()) ? 0 : (deletionList.size() - 1);
      deleteCursor = new int[] {cursor};
    }

    @Override
    protected void skipToCurrentTimeRangeStartPosition() {
      if (timeRange == null || index >= rows) {
        return;
      }
      if (timeRange.contains(getTime(getScanOrderIndex(index)))) {
        return;
      }

      int indexInTVList;
      // Since there may be duplicate timestamps in TVList, we need to move to the index of the
      // first timestamp that meets the requirements under current scanOrder.
      if (scanOrder.isAscending()) {
        long searchTimestamp = timeRange.getMin();
        if (searchTimestamp <= outer.getMinTime()) {
          return;
        }
        if (searchTimestamp > outer.getMaxTime()) {
          // all satisfied data has been consumed
          index = rows;
          probeNext = true;
          return;
        }
        // For asc scan, if it can not be found, the indexInTVList is too small, and we should move
        // to the next timestamp position.
        // If it can be found, move to the min index of current timestamp.
        indexInTVList =
            binarySearchTimestampFirstGreaterOrEqualsPosition(
                searchTimestamp, getScanOrderIndex(index), rows - 1);
      } else {
        long searchTimestamp = timeRange.getMax();
        if (searchTimestamp >= outer.getMaxTime()) {
          return;
        }
        if (searchTimestamp < outer.getMinTime()) {
          // all satisfied data has been consumed
          index = rows;
          probeNext = true;
          return;
        }
        // For desc scan, regardless of whether it is found, the timestamp corresponding to
        // indexInTVList has met the conditions. We only need to find the index that first
        // encounters this timestamp during desc scan.
        indexInTVList =
            binarySearchTimestampLastLessOrEqualsPosition(
                searchTimestamp, 0, getScanOrderIndex(index));
      }
      int newIndex = getScanOrderIndex(indexInTVList);
      if (newIndex > index) {
        index = newIndex;
      }

      probeNext = false;
    }

    protected void prepareNext() {
      if (scanOrder.isAscending()) {
        // For ASC traversal, we first find a valid index and then handle duplicate timestamps.
        // example:
        // index: 0 scanOrderIndex: 0 (time: 0 value: 0) (deleted)
        // index: 1 scanOrderIndex: 1 (time: 1 value: 1)
        // index: 2 scanOrderIndex: 2 (time: 1 value: 2)
        // index: 3 scanOrderIndex: 3 (time: 2 value: 2) (deleted)
        // we will move to index 2 finally, and the index track is (0 -> 1 -> 2)

        // skip deleted rows
        skipDeletedOrTimeNotSatisfiedRows();
        // skip duplicated timestamp
        while (index + 1 < rows
            && getTime(getScanOrderIndex(index + 1)) == getTime(getScanOrderIndex(index))) {
          index++;
        }
      } else {
        // For DESC traversal, we first handle duplicate timestamps and then find a valid index.
        // example:
        // index: 0 scanOrderIndex: 3 (time: 0 value: 0) (deleted)
        // index: 1 scanOrderIndex: 2 (time: 1 value: 1)
        // index: 2 scanOrderIndex: 1 (time: 1 value: 2)
        // index: 3 scanOrderIndex: 0 (time: 2 value: 2) (deleted)
        // we will move to index 2 finally, and the index track is (3 -> 2)

        // First skip the duplicate timestamps of the latest value that has been used in last call,
        // and then skip the deleted points. At this time, the first valid point we encounter is a
        // valid one.

        // skip duplicated timestamp
        while (index > 0
            && (index <= rows - 1)
            && getTime(getScanOrderIndex(index - 1)) == getTime(getScanOrderIndex(index))) {
          index++;
        }
        // skip deleted rows
        skipDeletedOrTimeNotSatisfiedRows();
      }
      probeNext = true;
    }

    protected void skipDeletedOrTimeNotSatisfiedRows() {
      while (index < rows) {
        if (!isNullValue(getValueIndex(getScanOrderIndex(index)))) {
          long time = getTime(getScanOrderIndex(index));
          if (!isPointDeleted(time, deletionList, deleteCursor, scanOrder)
              && isTimeSatisfied(time)) {
            break;
          }
        }
        index++;
      }
    }

    protected boolean isTimeSatisfied(long timestamp) {
      return globalTimeFilter == null || globalTimeFilter.satisfyRow(timestamp, null);
    }

    // When used as a point reader, we should not apply a pagination controller or push down filter
    // because it has not yet been merged with other data.
    @Override
    public boolean hasNextTimeValuePair() {
      if (!paginationController.hasCurLimit()) {
        return false;
      }
      if (!probeNext) {
        prepareNext();
      }
      return index < rows && !isCurrentTimeExceedTimeRange(getTime(getScanOrderIndex(index)));
    }

    @Override
    public TimeValuePair nextTimeValuePair() {
      if (!hasNextTimeValuePair()) {
        return null;
      }
      TimeValuePair tvp = getTimeValuePair(getScanOrderIndex(index));
      next();
      return tvp;
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      if (!hasNextTimeValuePair()) {
        return null;
      }
      return getTimeValuePair(getScanOrderIndex(index));
    }

    @Override
    public TsBlock getBatch(int tsBlockIndex) {
      if (tsBlockIndex < 0 || tsBlockIndex >= tsBlocks.size()) {
        return null;
      }
      return tsBlocks.get(tsBlockIndex);
    }

    @Override
    public boolean hasNextBatch() {
      if (!paginationController.hasCurLimit()) {
        return false;
      }
      return hasNextTimeValuePair();
    }

    @Override
    public TsBlock nextBatch() {
      TSDataType dataType = getDataType();
      int maxRowCountOfCurrentBatch =
          Math.min(
              paginationController.hasSetLimit()
                  ? (int) paginationController.getCurLimit()
                  : Integer.MAX_VALUE,
              Math.min(maxNumberOfPointsInPage, rows - index));
      TsBlockBuilder builder =
          new TsBlockBuilder(maxRowCountOfCurrentBatch, Collections.singletonList(dataType));
      switch (dataType) {
        case BOOLEAN:
          while (index < rows
              && builder.getPositionCount() < maxNumberOfPointsInPage
              && paginationController.hasCurLimit()) {
            long time = getTime(getScanOrderIndex(index));
            if (isCurrentTimeExceedTimeRange(time)) {
              break;
            }
            if (!isNullValue(getValueIndex(getScanOrderIndex(index)))
                && !isPointDeleted(time, deletionList, deleteCursor, scanOrder)
                && isLatestPoint(index, time)
                && isTimeSatisfied(time)) {
              boolean aBoolean = getBoolean(getScanOrderIndex(index));
              if (pushDownFilter == null || pushDownFilter.satisfyBoolean(time, aBoolean)) {
                if (paginationController.hasCurOffset()) {
                  paginationController.consumeOffset();
                  index++;
                  continue;
                }
                paginationController.consumeLimit();
                builder.getTimeColumnBuilder().writeLong(time);
                builder.getColumnBuilder(0).writeBoolean(aBoolean);
                builder.declarePosition();
              }
            }
            index++;
          }
          break;
        case INT32:
        case DATE:
          while (index < rows
              && builder.getPositionCount() < maxNumberOfPointsInPage
              && paginationController.hasCurLimit()) {
            long time = getTime(getScanOrderIndex(index));
            if (isCurrentTimeExceedTimeRange(time)) {
              break;
            }
            if (!isNullValue(getValueIndex(getScanOrderIndex(index)))
                && !isPointDeleted(time, deletionList, deleteCursor, scanOrder)
                && isLatestPoint(index, time)
                && isTimeSatisfied(time)) {
              int anInt = getInt(getScanOrderIndex(index));
              if (pushDownFilter == null || pushDownFilter.satisfyInteger(time, anInt)) {
                if (paginationController.hasCurOffset()) {
                  paginationController.consumeOffset();
                  index++;
                  continue;
                }
                paginationController.consumeLimit();
                builder.getTimeColumnBuilder().writeLong(time);
                builder.getColumnBuilder(0).writeInt(anInt);
                builder.declarePosition();
              }
            }
            index++;
          }
          break;
        case INT64:
        case TIMESTAMP:
          while (index < rows
              && builder.getPositionCount() < maxNumberOfPointsInPage
              && paginationController.hasCurLimit()) {
            long time = getTime(getScanOrderIndex(index));
            if (isCurrentTimeExceedTimeRange(time)) {
              break;
            }
            if (!isNullValue(getValueIndex(getScanOrderIndex(index)))
                && !isPointDeleted(time, deletionList, deleteCursor, scanOrder)
                && isLatestPoint(index, time)
                && isTimeSatisfied(time)) {
              long aLong = getLong(getScanOrderIndex(index));
              if (pushDownFilter == null || pushDownFilter.satisfyLong(time, aLong)) {
                if (paginationController.hasCurOffset()) {
                  paginationController.consumeOffset();
                  index++;
                  continue;
                }
                paginationController.consumeLimit();
                builder.getTimeColumnBuilder().writeLong(time);
                builder.getColumnBuilder(0).writeLong(aLong);
                builder.declarePosition();
              }
            }
            index++;
          }
          break;
        case FLOAT:
          while (index < rows
              && builder.getPositionCount() < maxNumberOfPointsInPage
              && paginationController.hasCurLimit()) {
            long time = getTime(getScanOrderIndex(index));
            if (isCurrentTimeExceedTimeRange(time)) {
              break;
            }
            if (!isNullValue(getValueIndex(getScanOrderIndex(index)))
                && !isPointDeleted(time, deletionList, deleteCursor, scanOrder)
                && isLatestPoint(index, time)
                && isTimeSatisfied(time)) {
              float aFloat =
                  roundValueWithGivenPrecision(
                      getFloat(getScanOrderIndex(index)), floatPrecision, encoding);
              if (pushDownFilter == null || pushDownFilter.satisfyFloat(time, aFloat)) {
                if (paginationController.hasCurOffset()) {
                  paginationController.consumeOffset();
                  index++;
                  continue;
                }
                paginationController.consumeLimit();
                builder.getTimeColumnBuilder().writeLong(time);
                builder.getColumnBuilder(0).writeFloat(aFloat);
                builder.declarePosition();
              }
            }
            index++;
          }
          break;
        case DOUBLE:
          while (index < rows
              && builder.getPositionCount() < maxNumberOfPointsInPage
              && paginationController.hasCurLimit()) {
            long time = getTime(getScanOrderIndex(index));
            if (isCurrentTimeExceedTimeRange(time)) {
              break;
            }
            if (!isNullValue(getValueIndex(getScanOrderIndex(index)))
                && !isPointDeleted(time, deletionList, deleteCursor, scanOrder)
                && isLatestPoint(index, time)
                && isTimeSatisfied(time)) {
              double aDouble =
                  roundValueWithGivenPrecision(
                      getDouble(getScanOrderIndex(index)), floatPrecision, encoding);
              if (pushDownFilter == null || pushDownFilter.satisfyDouble(time, aDouble)) {
                if (paginationController.hasCurOffset()) {
                  paginationController.consumeOffset();
                  index++;
                  continue;
                }
                paginationController.consumeLimit();
                builder.getTimeColumnBuilder().writeLong(time);
                builder.getColumnBuilder(0).writeDouble(aDouble);
                builder.declarePosition();
              }
            }
            index++;
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          while (index < rows
              && builder.getPositionCount() < maxNumberOfPointsInPage
              && paginationController.hasCurLimit()) {
            long time = getTime(getScanOrderIndex(index));
            if (isCurrentTimeExceedTimeRange(time)) {
              break;
            }
            if (!isNullValue(getValueIndex(getScanOrderIndex(index)))
                && !isPointDeleted(time, deletionList, deleteCursor, scanOrder)
                && isLatestPoint(index, time)
                && isTimeSatisfied(time)) {
              Binary binary = getBinary(getScanOrderIndex(index));
              if (pushDownFilter == null || pushDownFilter.satisfyBinary(time, binary)) {
                if (paginationController.hasCurOffset()) {
                  paginationController.consumeOffset();
                  index++;
                  continue;
                }
                paginationController.consumeLimit();
                builder.getTimeColumnBuilder().writeLong(time);
                builder.getColumnBuilder(0).writeBinary(binary);
                builder.declarePosition();
              }
            }
            index++;
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
      // There is no need to process pushDownFilter and paginationController here because it has
      // been applied when constructing the tsBlock
      TsBlock tsBlock = builder.build();
      addTsBlock(tsBlock);
      return tsBlock;
    }

    protected boolean isLatestPoint(int rowIndex, long currentTime) {
      if (scanOrder.isAscending()) {
        return rowIndex == rows - 1 || currentTime != getTime(getScanOrderIndex(rowIndex + 1));
      } else {
        return rowIndex == 0 || currentTime != getTime(getScanOrderIndex(rowIndex - 1));
      }
    }

    // When traversing in desc order, the index needs to be converted
    public int getScanOrderIndex(int rowIndex) {
      return scanOrder.isAscending() ? rowIndex : rows - 1 - rowIndex;
    }

    @Override
    public void encodeBatch(IChunkWriter chunkWriter, BatchEncodeInfo encodeInfo, long[] times) {
      TSDataType dataType = getDataType();
      ChunkWriterImpl chunkWriterImpl = (ChunkWriterImpl) chunkWriter;
      for (; index < rows; index++) {
        if (isNullValue(getValueIndex(index))) {
          continue;
        }
        long time = getTime(index);
        while (index + 1 < rows && time == getTime(index + 1)) {
          index++;
        }
        // store last point for SDT
        if (encodeInfo.lastIterator) {
          // skip deleted rows
          while (index < rows && isNullValue(getValueIndex(index))) {
            index++;
          }
          if (index == rows || index == rows - 1) {
            chunkWriterImpl.setLastPoint(true);
          }
        }

        switch (dataType) {
          case BOOLEAN:
            chunkWriterImpl.write(time, getBoolean(index));
            encodeInfo.dataSizeInChunk += 8L + 1L;
            break;
          case INT32:
          case DATE:
            chunkWriterImpl.write(time, getInt(index));
            encodeInfo.dataSizeInChunk += 8L + 4L;
            break;
          case INT64:
          case TIMESTAMP:
            chunkWriterImpl.write(time, getLong(index));
            encodeInfo.dataSizeInChunk += 8L + 8L;
            break;
          case FLOAT:
            chunkWriterImpl.write(time, getFloat(index));
            encodeInfo.dataSizeInChunk += 8L + 4L;
            break;
          case DOUBLE:
            chunkWriterImpl.write(time, getDouble(index));
            encodeInfo.dataSizeInChunk += 8L + 8L;
            break;
          case TEXT:
          case BLOB:
          case STRING:
          case OBJECT:
            Binary value = getBinary(index);
            chunkWriterImpl.write(time, value);
            encodeInfo.dataSizeInChunk += 8L + getBinarySize(value);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataType));
        }
        encodeInfo.pointNumInChunk++;
        if (encodeInfo.pointNumInChunk >= encodeInfo.maxNumberOfPointsInChunk
            || encodeInfo.dataSizeInChunk >= encodeInfo.targetChunkSize) {
          break;
        }
      }
    }

    @Override
    public long getUsedMemorySize() {
      return 0;
    }

    public void next() {
      index++;
      probeNext = false;
    }

    public boolean hasCurrent() {
      return index < rows;
    }

    public long currentTime() {
      if (!hasCurrent()) {
        return Long.MIN_VALUE;
      }
      return getTime(getScanOrderIndex(index));
    }

    public int getIndex() {
      return index;
    }

    public void setIndex(int index) {
      this.index = index;
      this.probeNext = false;
    }

    public void reset() {
      index = 0;
      probeNext = false;
    }

    public TVList getTVList() {
      return outer;
    }
  }

  protected static int getDefaultArrayNum() {
    if (System.currentTimeMillis() - defaultArrayNumLastUpdatedTimeMs > 10_000) {
      defaultArrayNumLastUpdatedTimeMs = System.currentTimeMillis();
      defaultArrayNum =
          ((int) WritingMetrics.getInstance().getAvgPointHistogram().takeSnapshot().getMean()
              / ARRAY_SIZE);
    }
    return defaultArrayNum;
  }
}
