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
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.MathUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
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

  // lock to provide synchronization for query list
  private final ReentrantLock queryListLock = new ReentrantLock();
  // list of query that this TVList is used
  protected final List<QueryContext> queryContextList;

  // the owner query which is obligated to release the TVList.
  // When it is null, the TVList is owned by insert thread and released after flush.
  protected QueryContext ownerQuery;

  protected boolean sorted = true;
  protected long maxTime;
  protected long minTime;
  // record reference count of this tv list
  // currently this reference will only be increase because we can't know when to decrease it
  protected AtomicInteger referenceCount;
  private long version;

  protected TVList() {
    timestamps = new ArrayList<>();
    indices = new ArrayList<>();
    rowCount = 0;
    seqRowCount = 0;
    maxTime = Long.MIN_VALUE;
    minTime = Long.MAX_VALUE;
    queryContextList = new ArrayList<>();
    referenceCount = new AtomicInteger();
  }

  public static TVList newList(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
      case BLOB:
      case STRING:
        return BinaryTVList.newList();
      case FLOAT:
        return FloatTVList.newList();
      case INT32:
      case DATE:
        return IntTVList.newList();
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

  public static long tvListArrayMemCost(TSDataType type) {
    long size = 0;
    // time array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * 8L;
    // value array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * (long) type.getDataTypeSize();
    // index array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * 4L;
    // bimap array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE / 8 + 1L;
    // two array headers mem size
    size += NUM_BYTES_ARRAY_HEADER * 2L;
    // Object references size in ArrayList
    size += NUM_BYTES_OBJECT_REF * 2L;
    return size;
  }

  public long calculateRamSize() {
    return timestamps.size() * tvListArrayMemCost(getDataType());
  }

  public synchronized boolean isSorted() {
    return sorted;
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
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return indices.get(arrayIndex)[elementIndex];
  }

  protected void markNullValue(int arrayIndex, int elementIndex) {
    // init bitMap if doesn't have
    if (bitMap == null) {
      bitMap = new ArrayList<>();
      for (int i = 0; i < timestamps.size(); i++) {
        bitMap.add(new BitMap(ARRAY_SIZE));
      }
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
      cloneList.bitMap = new ArrayList<>();
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

  protected abstract void releaseLastValueArray();

  protected void releaseLastTimeArray() {
    PrimitiveArrayManager.release(timestamps.remove(timestamps.size() - 1));
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
    for (int[] indicesArray : indices) {
      cloneList.indices.add(cloneIndex(indicesArray));
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
    queryContextList.clear();
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
      minTime = Math.min(minTime, time[i]);
      if (inputSorted) {
        if (i < length - 1 && time[i] > time[i + 1]) {
          inputSorted = false;
        } else {
          inputSeqRowCount++;
        }
      }
    }
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
      case STRING:
        return BinaryTVList.deserialize(stream);
      case FLOAT:
        return FloatTVList.deserialize(stream);
      case INT32:
      case DATE:
        return IntTVList.deserialize(stream);
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
        return BinaryTVList.deserializeWithoutBitMap(stream);
      case FLOAT:
        return FloatTVList.deserializeWithoutBitMap(stream);
      case INT32:
      case DATE:
        return IntTVList.deserializeWithoutBitMap(stream);
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

  public void setOwnerQuery(QueryContext queryCtx) {
    this.ownerQuery = queryCtx;
  }

  public QueryContext getOwnerQuery() {
    return ownerQuery;
  }

  public List<QueryContext> getQueryContextList() {
    return queryContextList;
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

  public TVListIterator iterator(Integer floatPrecision, TSEncoding encoding) {
    return new TVListIterator(floatPrecision, encoding);
  }

  /* TVList Iterator */
  public class TVListIterator {
    protected int index;
    protected int rows;
    protected long currentTime;
    protected boolean probeNext;
    private Integer floatPrecision;
    private TSEncoding encoding;

    public TVListIterator() {}

    public TVListIterator(Integer floatPrecision, TSEncoding encoding) {
      this.index = 0;
      this.rows = rowCount;
      this.currentTime = index < rows ? getTime(index) : Long.MIN_VALUE;
      this.floatPrecision = floatPrecision;
      this.encoding = encoding;
    }

    private void prepareNext() {
      // skip deleted rows
      int prevIndex = index;
      while (index < rows && (bitMap != null && isNullValue(getValueIndex(index)))) {
        index++;
      }
      // update current timestamp if needed
      if (index > prevIndex) {
        currentTime = index < rows ? getTime(index) : Long.MIN_VALUE;
      }

      // skip duplicated timestamp
      while (index + 1 < rows && getTime(index + 1) == currentTime) {
        index++;
      }
      probeNext = true;
    }

    public boolean hasNext() {
      if (!probeNext) {
        prepareNext();
      }
      return index < rows;
    }

    public TimeValuePair next() {
      if (!hasNext()) {
        return null;
      }
      TimeValuePair ret = getTimeValuePair(index++, currentTime, floatPrecision, encoding);
      currentTime = index < rows ? getTime(index) : Long.MIN_VALUE;
      probeNext = false;
      return ret;
    }

    public TimeValuePair current() {
      if (!hasCurrent()) {
        return null;
      }
      return getTimeValuePair(index, currentTime, floatPrecision, encoding);
    }

    public boolean hasCurrent() {
      if (bitMap == null) {
        return index < rows;
      }
      return index < rows && !isNullValue(getValueIndex(index));
    }

    public long currentTime() {
      if (!hasCurrent()) {
        return Long.MIN_VALUE;
      }
      return currentTime;
    }

    public int getIndex() {
      return index;
    }

    public void setIndex(int index) {
      this.index = index;
      this.probeNext = false;
      this.currentTime = index < rows ? getTime(index) : Long.MIN_VALUE;
    }

    protected void step() {
      index++;
      probeNext = false;
      currentTime = index < rows ? getTime(index) : Long.MIN_VALUE;
    }

    public void reset() {
      index = 0;
      currentTime = index < rows ? getTime(index) : Long.MIN_VALUE;
      probeNext = false;
    }

    @Override
    public TVListIterator clone() {
      TVListIterator iterator = new TVListIterator();
      iterator.rows = rows;
      iterator.floatPrecision = floatPrecision;
      iterator.encoding = encoding;
      iterator.reset();
      return iterator;
    }
  }
}
