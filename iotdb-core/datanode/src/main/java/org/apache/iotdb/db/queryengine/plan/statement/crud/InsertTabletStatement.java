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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertTablet;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class InsertTabletStatement extends InsertBaseStatement implements ISchemaValidation {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTabletStatement.class);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InsertTabletStatement.class);

  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  protected TimeView times; // times should be sorted. It is done in the session API.
  protected BitMap[] nullBitMaps;
  protected ValueView columns;

  protected IDeviceID[] deviceIDs;

  protected boolean singleDevice;

  protected int rowCount = 0;

  protected AtomicInteger refCount;

  /**
   * This param record whether the source of logical view is aligned. Only used when there are
   * views.
   */
  protected boolean[] measurementIsAligned;

  public InsertTabletStatement() {
    super();
    statementType = StatementType.BATCH_INSERT;
    this.recordedBeginOfLogicalViewSchemaList = 0;
    this.recordedEndOfLogicalViewSchemaList = 0;
  }

  public InsertTabletStatement(InsertTabletNode node) {
    this();
    setDevicePath(node.getTargetPath());
    setMeasurements(node.getMeasurements());
    setTimes(node.getTimes());
    setColumns(node.getColumns());
    setBitMaps(node.getBitMaps());
    setRowCount(node.getRowCount());
    setDataTypes(node.getDataTypes());
    setAligned(node.isAligned());
    setMeasurementSchemas(node.getMeasurementSchemas());
  }

  public InsertTabletStatement(RelationalInsertTabletNode node) {
    this(((InsertTabletNode) node));
    setColumnCategories(node.getColumnCategories());
    setWriteToTable(true);
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public ValueView getColumns() {
    return columns;
  }

  public void setColumns(ValueView columns) {
    this.columns = columns;
  }

  public void setColumns(Object[] columns) {
    if (dataTypes == null || rowCount == 0) {
      throw new IllegalArgumentException("dataTypes and rowCount must be set first");
    }
    this.columns = new TwoDArrayValueView(columns, this::getDataTypes, rowCount);
  }

  public BitMap[] getBitMaps() {
    return nullBitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.nullBitMaps = bitMaps;
  }

  public TimeView getTimes() {
    return times;
  }

  public void setTimes(TimeView times) {
    this.times = times;
  }

  public void setTimes(long[] times) {
    this.times = new SingleArrayTimeView(times);
  }

  @Override
  public boolean isEmpty() {
    return rowCount == 0
        || times.length() == 0
        || measurements.length == 0
        || dataTypes.length == 0
        || columns.colLength() == 0;
  }

  public List<TTimePartitionSlot> getTimePartitionSlots() {
    List<TTimePartitionSlot> result = new ArrayList<>();
    long upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times.get(0));
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times.get(0));
    for (int i = 1; i < times.length(); i++) { // times are sorted in session API.
      long time = times.get(i);
      if (time >= upperBoundOfTimePartition) {
        result.add(timePartitionSlot);
        // next init
        upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(time);
        timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(time);
      }
    }
    result.add(timePartitionSlot);
    return result;
  }

  public TTimePartitionSlot getTimePartitionSlot(int i) {
    return TimePartitionUtils.getTimePartitionSlot(times.get(i));
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertTablet(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurements) {
      PartialPath fullPath = devicePath.concatAsMeasurementPath(m);
      ret.add(fullPath);
    }
    return ret;
  }

  @Override
  public ISchemaValidation getSchemaValidation() {
    return this;
  }

  @Override
  public List<ISchemaValidation> getSchemaValidationList() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    if (dataType.isCompatible(dataTypes[columnIndex])) {
      columns.castTo(columnIndex, dataType);
      dataTypes[columnIndex] = dataType;
      return true;
    }
    return false;
  }

  @Override
  public void markFailedMeasurement(int index, Exception cause) {
    if (measurements[index] == null) {
      return;
    }

    if (failedMeasurementIndex2Info == null) {
      failedMeasurementIndex2Info = new HashMap<>();
    }

    InsertBaseStatement.FailedMeasurementInfo failedMeasurementInfo =
        new InsertBaseStatement.FailedMeasurementInfo(measurements[index], dataTypes[index], cause);
    failedMeasurementIndex2Info.putIfAbsent(index, failedMeasurementInfo);

    measurements[index] = null;
    dataTypes[index] = null;
  }

  @Override
  public void removeAllFailedMeasurementMarks() {
    if (failedMeasurementIndex2Info == null) {
      return;
    }
    failedMeasurementIndex2Info.forEach(
        (index, info) -> {
          measurements[index] = info.getMeasurement();
          dataTypes[index] = info.getDataType();
        });
    failedMeasurementIndex2Info.clear();
  }

  @Override
  public void semanticCheck() {
    super.semanticCheck();
    if (measurements.length != columns.colLength()) {
      throw new SemanticException(
          String.format(
              "the measurementList's size %d is not consistent with the columnList's size %d",
              measurements.length, columns.colLength()));
    }
  }

  public boolean isNeedSplit() {
    return hasLogicalViewNeedProcess();
  }

  public List<InsertTabletStatement> getSplitList() {
    if (!isNeedSplit()) {
      return Collections.singletonList(this);
    }
    Map<PartialPath, List<Pair<String, Integer>>> mapFromDeviceToMeasurementAndIndex =
        this.getMapFromDeviceToMeasurementAndIndex();
    // Reconstruct statements
    List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();
    for (Map.Entry<PartialPath, List<Pair<String, Integer>>> entry :
        mapFromDeviceToMeasurementAndIndex.entrySet()) {
      List<Pair<String, Integer>> pairList = entry.getValue();
      InsertTabletStatement statement = new InsertTabletStatement();
      statement.setTimes(this.times);
      statement.setDevicePath(entry.getKey());
      statement.setRowCount(this.rowCount);
      statement.setAligned(this.isAligned);
      String[] measurements = new String[pairList.size()];
      BitMap[] copiedBitMaps = new BitMap[pairList.size()];
      MeasurementSchema[] measurementSchemas = new MeasurementSchema[pairList.size()];
      TSDataType[] dataTypes = new TSDataType[pairList.size()];
      for (int i = 0; i < pairList.size(); i++) {
        int realIndex = pairList.get(i).right;
        measurements[i] =
            Objects.nonNull(this.measurements[realIndex]) ? pairList.get(i).left : null;
        measurementSchemas[i] = this.measurementSchemas[realIndex];
        dataTypes[i] = this.dataTypes[realIndex];
        if (this.nullBitMaps != null) {
          copiedBitMaps[i] = this.nullBitMaps[realIndex];
        }
        if (this.measurementIsAligned != null) {
          statement.setAligned(this.measurementIsAligned[realIndex]);
        }
      }
      statement.setColumns(
          new ColumnMappedValueView(
              columns, pairList.stream().map(Pair::getRight).collect(Collectors.toList())));
      statement.setMeasurements(measurements);
      statement.setMeasurementSchemas(measurementSchemas);
      statement.setDataTypes(dataTypes);
      if (this.nullBitMaps != null) {
        statement.setBitMaps(copiedBitMaps);
      }
      statement.setFailedMeasurementIndex2Info(failedMeasurementIndex2Info);
      insertTabletStatementList.add(statement);
    }
    return insertTabletStatementList;
  }

  @Override
  public InsertBaseStatement removeLogicalView() {
    if (!isNeedSplit()) {
      return this;
    }
    List<InsertTabletStatement> insertTabletStatementList = this.getSplitList();
    if (insertTabletStatementList.size() == 1) {
      return insertTabletStatementList.get(0);
    }
    InsertMultiTabletsStatement insertMultiTabletsStatement = new InsertMultiTabletsStatement();
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletStatementList);
    return insertMultiTabletsStatement;
  }

  @Override
  public long getMinTime() {
    return times.get(0);
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    return columns.get(0, index);
  }

  @Override
  public TSDataType getDataType(int index) {
    return dataTypes != null ? dataTypes[index] : null;
  }

  @Override
  public TSEncoding getEncoding(int index) {
    return null;
  }

  @Override
  public CompressionType getCompressionType(int index) {
    return null;
  }

  @Override
  public void validateDeviceSchema(boolean isAligned) {
    this.isAligned = isAligned;
  }

  @Override
  public void validateMeasurementSchema(int index, IMeasurementSchemaInfo measurementSchemaInfo) {
    if (measurementSchemas == null) {
      measurementSchemas = new MeasurementSchema[measurements.length];
    }
    if (measurementSchemaInfo == null) {
      measurementSchemas[index] = null;
    } else {
      if (measurementSchemaInfo.isLogicalView()) {
        if (logicalViewSchemaList == null || indexOfSourcePathsOfLogicalViews == null) {
          logicalViewSchemaList = new ArrayList<>();
          indexOfSourcePathsOfLogicalViews = new ArrayList<>();
        }
        logicalViewSchemaList.add(measurementSchemaInfo.getSchemaAsLogicalViewSchema());
        indexOfSourcePathsOfLogicalViews.add(index);
        return;
      } else {
        measurementSchemas[index] = measurementSchemaInfo.getSchemaAsMeasurementSchema();
      }
    }

    try {
      selfCheckDataTypes(index);
    } catch (DataTypeMismatchException | PathNotExistException e) {
      throw new SemanticException(e);
    }
  }

  @Override
  public void validateMeasurementSchema(
      int index, IMeasurementSchemaInfo measurementSchemaInfo, boolean isAligned) {
    this.validateMeasurementSchema(index, measurementSchemaInfo);
    if (this.measurementIsAligned == null) {
      this.measurementIsAligned = new boolean[this.measurements.length];
      Arrays.fill(this.measurementIsAligned, this.isAligned);
    }
    this.measurementIsAligned[index] = isAligned;
  }

  @Override
  public boolean hasLogicalViewNeedProcess() {
    if (this.indexOfSourcePathsOfLogicalViews == null) {
      return false;
    }
    return !this.indexOfSourcePathsOfLogicalViews.isEmpty();
  }

  @Override
  public List<LogicalViewSchema> getLogicalViewSchemaList() {
    return this.logicalViewSchemaList;
  }

  @Override
  public List<Integer> getIndexListOfLogicalViewPaths() {
    return this.indexOfSourcePathsOfLogicalViews;
  }

  @Override
  public void recordRangeOfLogicalViewSchemaListNow() {
    if (this.logicalViewSchemaList != null) {
      this.recordedBeginOfLogicalViewSchemaList = this.recordedEndOfLogicalViewSchemaList;
      this.recordedEndOfLogicalViewSchemaList = this.logicalViewSchemaList.size();
    }
  }

  @Override
  public Pair<Integer, Integer> getRangeOfLogicalViewSchemaListRecorded() {
    return new Pair<>(
        this.recordedBeginOfLogicalViewSchemaList, this.recordedEndOfLogicalViewSchemaList);
  }

  @Override
  public Statement toRelationalStatement(MPPQueryContext context) {
    return new InsertTablet(this, context);
  }

  public IDeviceID getTableDeviceID(int rowIdx) {
    if (deviceIDs == null) {
      deviceIDs = new IDeviceID[rowCount];
    }
    if (deviceIDs[rowIdx] == null) {
      String[] deviceIdSegments = new String[getIdColumnIndices().size() + 1];
      deviceIdSegments[0] = this.getTableName();
      for (int i = 0; i < getIdColumnIndices().size(); i++) {
        final Integer columnIndex = getIdColumnIndices().get(i);
        boolean isNull = isNull(rowIdx, i);
        deviceIdSegments[i + 1] = isNull ? null : columns.get(rowIdx, columnIndex).toString();
      }
      deviceIDs[rowIdx] = Factory.DEFAULT_FACTORY.create(deviceIdSegments);
    }

    return deviceIDs[rowIdx];
  }

  public IDeviceID[] getRawTableDeviceIDs() {
    return deviceIDs;
  }

  public void setSingleDevice() {
    singleDevice = true;
  }

  public boolean isSingleDevice() {
    return singleDevice;
  }

  @Override
  public void insertColumn(int pos, ColumnSchema columnSchema) {
    super.insertColumn(pos, columnSchema);

    if (nullBitMaps == null) {
      nullBitMaps = new BitMap[measurements.length];
      nullBitMaps[pos] = new BitMap(rowCount);
      for (int i = 0; i < rowCount; i++) {
        nullBitMaps[pos].mark(i);
      }
    } else {
      BitMap[] tmpBitmaps = new BitMap[nullBitMaps.length + 1];
      System.arraycopy(nullBitMaps, 0, tmpBitmaps, 0, pos);
      tmpBitmaps[pos] = new BitMap(rowCount);
      for (int i = 0; i < rowCount; i++) {
        tmpBitmaps[pos].mark(i);
      }
      System.arraycopy(nullBitMaps, pos, tmpBitmaps, pos + 1, nullBitMaps.length - pos);
      nullBitMaps = tmpBitmaps;
    }

    columns.insertColumn(pos, columnSchema);

    deviceIDs = null;
  }

  @Override
  public void swapColumn(int src, int target) {
    super.swapColumn(src, target);
    if (nullBitMaps != null) {
      CommonUtils.swapArray(nullBitMaps, src, target);
    }
    columns.swapColumn(src, target);
    deviceIDs = null;
  }

  @Override
  protected long calculateBytesUsed() {
    return INSTANCE_SIZE
        + times.ramSize()
        + InsertNodeMemoryEstimator.sizeOfBitMapArray(nullBitMaps)
        + columns.ramSize(measurementSchemas)
        + (Objects.nonNull(deviceIDs)
            ? Arrays.stream(deviceIDs)
                .mapToLong(InsertNodeMemoryEstimator::sizeOfIDeviceID)
                .reduce(0L, Long::sum)
            : 0L);
  }

  public boolean isNull(int row, int col) {
    if (nullBitMaps == null || nullBitMaps[col] == null) {
      return false;
    }
    return nullBitMaps[col].isMarked(row);
  }

  @Override
  protected void reserveColumns(List<Integer> columnsToKeep) {
    if (columns != null) {
      columns.reserveColumns(columnsToKeep);
    }
    if (nullBitMaps != null) {
      nullBitMaps = columnsToKeep.stream().map(i -> nullBitMaps[i]).toArray(BitMap[]::new);
    }
  }

  public void setRefCount(AtomicInteger refCount) {
    this.refCount = refCount;
  }

  public void incRefCount() {
    if (refCount != null) {
      refCount.incrementAndGet();
    }
  }

  public void decRefCount() {
    if (refCount != null) {
      int after = refCount.decrementAndGet();
      if (after == 0) {
        times.release();
        columns.release();
      }
    }
  }

  public AtomicInteger getRefCount() {
    return refCount;
  }

  public interface TimeView {

    long get(int index);

    void set(int index, long value);

    int length();

    long ramSize();

    default void copyTo(TimeView timeView, int thisFrom, int targetFrom, int copyLength) {
      copyLength = Math.min(copyLength, this.length() - thisFrom);
      for (int i = 0; i < copyLength; i++) {
        timeView.set(targetFrom + i, get(thisFrom + i));
      }
    }

    void release();

    void putTo(TVList tvList, BitMap bitMap, int start, int end);

    // for compatibility only, avoid using it
    @Deprecated
    long[] toSingleArray();
  }

  public static class SingleArrayTimeView implements TimeView {

    private final long[] timestamps;

    public SingleArrayTimeView(long[] timestamps) {
      this.timestamps = timestamps;
    }

    @Override
    public long get(int index) {
      return timestamps[index];
    }

    @Override
    public int length() {
      return timestamps.length;
    }

    @Override
    public long ramSize() {
      return RamUsageEstimator.sizeOfObject(timestamps);
    }

    @Override
    public void set(int index, long value) {
      timestamps[index] = value;
    }

    @Override
    public void copyTo(TimeView timeView, int thisFrom, int targetFrom, int copyLength) {
      if (timeView instanceof SingleArrayTimeView) {
        copyLength = Math.min(copyLength, this.length() - thisFrom);
        System.arraycopy(
            this.timestamps,
            thisFrom,
            ((SingleArrayTimeView) timeView).timestamps,
            targetFrom,
            copyLength);
      } else {
        TimeView.super.copyTo(timeView, thisFrom, targetFrom, copyLength);
      }
    }

    @Override
    public void release() {
      // do nothing
    }

    @Override
    public void putTo(TVList tvList, BitMap bitMap, int start, int end) {
      tvList.putTimes(timestamps, bitMap, start, end);
    }

    @Override
    public long[] toSingleArray() {
      return timestamps;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SingleArrayTimeView that = (SingleArrayTimeView) o;
      return Objects.deepEquals(timestamps, that.timestamps);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(timestamps);
    }
  }

  public static class MultiArrayTimeView implements TimeView {

    private final int singleArraySize;
    private final long[][] timestamps;
    private final int length;

    public MultiArrayTimeView(int singleArraySize, long[][] timestamps, int length) {
      this.singleArraySize = singleArraySize;
      this.timestamps = timestamps;
      this.length = length;
    }

    @Override
    public long get(int index) {
      return timestamps[index / singleArraySize][index % singleArraySize];
    }

    @Override
    public int length() {
      return length;
    }

    @Override
    public long ramSize() {
      return RamUsageEstimator.sizeOfObject(timestamps);
    }

    @Override
    public void set(int index, long value) {
      timestamps[index / singleArraySize][index % singleArraySize] = value;
    }

    @Override
    public void copyTo(TimeView timeView, int thisFrom, int targetFrom, int copyLength) {
      if (timeView instanceof MultiArrayTimeView
          && this.singleArraySize == ((MultiArrayTimeView) timeView).singleArraySize) {
        copyLength = Math.min(copyLength, this.length() - thisFrom);
        while (copyLength > 0) {
          int singleCopyLength =
              copyOneArrayTo(((MultiArrayTimeView) timeView), thisFrom, targetFrom, copyLength);
          thisFrom += singleCopyLength;
          targetFrom += singleCopyLength;
          copyLength -= singleCopyLength;
        }
      } else {
        TimeView.super.copyTo(timeView, thisFrom, targetFrom, copyLength);
      }
    }

    private int copyOneArrayTo(
        MultiArrayTimeView target, int thisFrom, int targetFrom, int copyLength) {
      int srcArrayPos = thisFrom % singleArraySize;
      int srcArrayRemaining = singleArraySize - srcArrayPos;
      copyLength = Math.min(copyLength, srcArrayRemaining);
      long[] srcArray = timestamps[thisFrom / singleArraySize];
      long[] targetArray = target.timestamps[targetFrom / singleArraySize];
      int targetArrayPos = targetFrom % singleArraySize;
      int targetArrayRemaining = singleArraySize - targetArrayPos;

      if (copyLength > targetArrayRemaining) {
        System.arraycopy(srcArray, srcArrayPos, targetArray, targetArrayPos, targetArrayRemaining);
        long[] nextTargetArray = target.timestamps[targetFrom / singleArraySize + 1];
        System.arraycopy(
            srcArray,
            srcArrayPos + targetArrayRemaining,
            nextTargetArray,
            0,
            copyLength - targetArrayRemaining);
      } else {
        System.arraycopy(srcArray, srcArrayPos, targetArray, targetArrayPos, copyLength);
      }
      return copyLength;
    }

    @Override
    public void release() {
      for (long[] timestamp : timestamps) {
        PrimitiveArrayManager.release(timestamp);
      }
    }

    @Override
    public void putTo(TVList tvList, BitMap bitMap, int start, int end) {
      if (end > length) {
        end = length;
      }

      int current = start;
      while (current < end) {
        // put one array to TVList
        int arrayIndex = current / singleArraySize;
        int arrayRemaining = singleArraySize - current % singleArraySize;
        int copyLength = Math.min(arrayRemaining, end - current);

        int arrayStart = current % singleArraySize;
        int arrayEnd = arrayStart + copyLength;
        tvList.putTimes(
            timestamps[arrayIndex],
            bitMap == null ? null : bitMap.getRegion(current, current + copyLength),
            arrayStart,
            arrayEnd);

        current += copyLength;
      }

      LOGGER.info("Put {} values into list {}", end - start, System.identityHashCode(tvList));
    }

    @Override
    public long[] toSingleArray() {
      long[] singleArray = new long[length];
      int arrayIndex = 0;
      for (; arrayIndex < timestamps.length - 1; arrayIndex++) {
        System.arraycopy(
            timestamps[arrayIndex], 0, singleArray, arrayIndex * singleArraySize, singleArraySize);
      }
      System.arraycopy(
          timestamps[arrayIndex],
          0,
          singleArray,
          arrayIndex * singleArraySize,
          length % singleArraySize);
      return singleArray;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MultiArrayTimeView that = (MultiArrayTimeView) o;
      return singleArraySize == that.singleArraySize
          && length == that.length
          && Objects.deepEquals(timestamps, that.timestamps);
    }

    @Override
    public int hashCode() {
      return Objects.hash(singleArraySize, Arrays.deepHashCode(timestamps), length);
    }
  }

  public interface ValueView {

    int colLength();

    int rowLength();

    Object get(int rowIndex, int colIndex);

    void set(int rowIndex, int colIndex, Object value);

    TSDataType[] dataTypes();

    default void copyTo(
        ValueView valueView, int colIndex, int thisFrom, int targetFrom, int copyLength) {
      copyLength = Math.min(copyLength, this.rowLength() - thisFrom);
      for (int i = 0; i < copyLength; i++) {
        valueView.set(targetFrom + i, colIndex, get(thisFrom + i, colIndex));
      }
    }

    void serializeColumn(int colIndex, ByteBuffer buffer);

    void serializeColumn(int colIndex, DataOutputStream stream) throws IOException;

    void serializeColumn(int colIndex, IWALByteBufferView buffer, int start, int end);

    default long getColumnSize(int colIndex, int start, int end) {
      if (dataTypes()[colIndex] == null) {
        return 0;
      }
      long size = 0;
      switch (dataTypes()[colIndex]) {
        case INT32:
        case DATE:
          size += Integer.BYTES * (end - start);
          break;
        case INT64:
        case TIMESTAMP:
          size += Long.BYTES * (end - start);
          break;
        case FLOAT:
          size += Float.BYTES * (end - start);
          break;
        case DOUBLE:
          size += Double.BYTES * (end - start);
          break;
        case BOOLEAN:
          size += Byte.BYTES * (end - start);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          for (int j = start; j < end; j++) {
            size += ReadWriteIOUtils.sizeToWrite((Binary) get(j, colIndex));
          }
          break;
      }
      return size;
    }

    void castTo(int colIndex, TSDataType newType);

    void insertColumn(int pos, ColumnSchema columnSchema);

    void swapColumn(int src, int target);

    long ramSize(MeasurementSchema[] measurementSchemas);

    void reserveColumns(List<Integer> columnsToReserve);

    void release();

    void putTo(TVList tvList, int columnIndex, BitMap bitMap, int start, int end, int pos);

    void putTo(
        AlignedTVList tvList,
        List<Integer> columnIndices,
        BitMap[] bitMaps,
        int start,
        int end,
        TSStatus[] results,
        int pos);

    // for compatibility only, do no use it in new code
    @Deprecated
    Object[] toTwoDArray();
  }

  public static class TwoDArrayValueView implements ValueView {

    private final Supplier<TSDataType[]> dataTypes;
    private Object[] values;
    private final int rowLength;

    public TwoDArrayValueView(Object[] values, Supplier<TSDataType[]> dataTypes, int rowLength) {
      this.values = values;
      this.dataTypes = dataTypes;
      this.rowLength = rowLength;
    }

    @Override
    public int colLength() {
      return values.length;
    }

    @Override
    public int rowLength() {
      return rowLength;
    }

    @Override
    public Object get(int rowIndex, int colIndex) {
      switch (dataTypes.get()[colIndex]) {
        case INT32:
          return ((int[]) values[colIndex])[rowIndex];
        case DATE:
          if (values[colIndex] instanceof int[]) {
            return ((int[]) values[colIndex])[rowIndex];
          } else if (values[colIndex] instanceof LocalDate[]) {
            return ((LocalDate[]) values[colIndex])[rowIndex];
          } else {
            throw new IllegalArgumentException(
                values[colIndex].getClass() + " is not a legal array for INT32");
          }
        case INT64:
        case TIMESTAMP:
          return ((long[]) values[colIndex])[rowIndex];
        case FLOAT:
          return ((float[]) values[colIndex])[rowIndex];
        case DOUBLE:
          return ((double[]) values[colIndex])[rowIndex];
        case TEXT:
        case BLOB:
        case STRING:
          return ((Binary[]) values[colIndex])[rowIndex];
        case BOOLEAN:
          return ((boolean[]) values[colIndex])[rowIndex];
        case VECTOR:
        case UNKNOWN:
        default:
          throw new IllegalArgumentException(dataTypes.get()[colIndex].toString());
      }
    }

    @Override
    public void set(int rowIndex, int colIndex, Object value) {
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          ((int[]) values[colIndex])[rowIndex] = ((int) value);
          return;
        case INT64:
        case TIMESTAMP:
          ((long[]) values[colIndex])[rowIndex] = ((long) value);
          return;
        case FLOAT:
          ((float[]) values[colIndex])[rowIndex] = ((float) value);
          return;
        case DOUBLE:
          ((double[]) values[colIndex])[rowIndex] = ((double) value);
          return;
        case TEXT:
        case BLOB:
        case STRING:
          ((Binary[]) values[colIndex])[rowIndex] = ((Binary) value);
          return;
        case BOOLEAN:
          ((boolean[]) values[colIndex])[rowIndex] = ((boolean) value);
          return;
        case VECTOR:
        case UNKNOWN:
        default:
          throw new IllegalArgumentException(dataTypes.get()[colIndex].toString());
      }
    }

    @Override
    public void copyTo(
        ValueView valueView, int colIndex, int thisFrom, int targetFrom, int copyLength) {
      if (valueView instanceof TwoDArrayValueView) {
        copyLength = Math.min(copyLength, this.rowLength() - thisFrom);
        System.arraycopy(
            values[colIndex],
            thisFrom,
            ((TwoDArrayValueView) valueView).values[colIndex],
            targetFrom,
            copyLength);
      } else {
        ValueView.super.copyTo(valueView, colIndex, thisFrom, targetFrom, copyLength);
      }
    }

    @Override
    public void serializeColumn(int colIndex, ByteBuffer buffer) {
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          int[] intValues = (int[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(intValues[j], buffer);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = (long[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(longValues[j], buffer);
          }
          break;
        case FLOAT:
          float[] floatValues = (float[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(floatValues[j], buffer);
          }
          break;
        case DOUBLE:
          double[] doubleValues = (double[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(doubleValues[j], buffer);
          }
          break;
        case BOOLEAN:
          boolean[] boolValues = (boolean[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(BytesUtils.boolToByte(boolValues[j]), buffer);
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary[] binaryValues = (Binary[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            if (binaryValues[j] != null) {
              ReadWriteIOUtils.write(binaryValues[j], buffer);
            } else {
              ReadWriteIOUtils.write(0, buffer);
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(DATATYPE_UNSUPPORTED, dataTypes.get()[colIndex]));
      }
    }

    @Override
    public void serializeColumn(int colIndex, DataOutputStream stream) throws IOException {
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          int[] intValues = (int[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(intValues[j], stream);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = (long[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(longValues[j], stream);
          }
          break;
        case FLOAT:
          float[] floatValues = (float[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(floatValues[j], stream);
          }
          break;
        case DOUBLE:
          double[] doubleValues = (double[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(doubleValues[j], stream);
          }
          break;
        case BOOLEAN:
          boolean[] boolValues = (boolean[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            ReadWriteIOUtils.write(BytesUtils.boolToByte(boolValues[j]), stream);
          }
          break;
        case STRING:
        case TEXT:
        case BLOB:
          Binary[] binaryValues = (Binary[]) values[colIndex];
          for (int j = 0; j < rowLength; j++) {
            if (binaryValues[j] != null) {
              ReadWriteIOUtils.write(binaryValues[j], stream);
            } else {
              ReadWriteIOUtils.write(0, stream);
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(DATATYPE_UNSUPPORTED, dataTypes.get()[colIndex]));
      }
    }

    @Override
    public void serializeColumn(int colIndex, IWALByteBufferView buffer, int start, int end) {
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          int[] intValues = (int[]) values[colIndex];
          for (int j = start; j < end; j++) {
            buffer.putInt(intValues[j]);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = (long[]) values[colIndex];
          for (int j = start; j < end; j++) {
            buffer.putLong(longValues[j]);
          }
          break;
        case FLOAT:
          float[] floatValues = (float[]) values[colIndex];
          for (int j = start; j < end; j++) {
            buffer.putFloat(floatValues[j]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = (double[]) values[colIndex];
          for (int j = start; j < end; j++) {
            buffer.putDouble(doubleValues[j]);
          }
          break;
        case BOOLEAN:
          boolean[] boolValues = (boolean[]) values[colIndex];
          for (int j = start; j < end; j++) {
            buffer.put(BytesUtils.boolToByte(boolValues[j]));
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary[] binaryValues = (Binary[]) values[colIndex];
          for (int j = start; j < end; j++) {
            if (binaryValues[j] != null) {
              buffer.putInt(binaryValues[j].getLength());
              buffer.put(binaryValues[j].getValues());
            } else {
              buffer.putInt(0);
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(DATATYPE_UNSUPPORTED, dataTypes.get()[colIndex]));
      }
    }

    @Override
    public TSDataType[] dataTypes() {
      return dataTypes.get();
    }

    @Override
    public void castTo(int colIndex, TSDataType newType) {
      values[colIndex] = newType.castFromArray(dataTypes.get()[colIndex], values[colIndex]);
    }

    @Override
    public void insertColumn(int pos, ColumnSchema columnSchema) {
      Object[] tmpColumns = new Object[values.length + 1];
      System.arraycopy(values, 0, tmpColumns, 0, pos);
      tmpColumns[pos] =
          CommonUtils.createValueColumnOfDataType(
              InternalTypeManager.getTSDataType(columnSchema.getType()), rowLength);
      System.arraycopy(values, pos, tmpColumns, pos + 1, values.length - pos);
      values = tmpColumns;
    }

    @Override
    public void swapColumn(int src, int target) {
      CommonUtils.swapArray(values, src, target);
    }

    @Override
    public long ramSize(MeasurementSchema[] measurementSchemas) {
      return InsertNodeMemoryEstimator.sizeOfColumns(values, measurementSchemas, null);
    }

    @Override
    public void reserveColumns(List<Integer> columnsToReserve) {
      values = columnsToReserve.stream().map(i -> values[i]).toArray();
    }

    @Override
    public void release() {
      // do nothing
    }

    @Override
    public void putTo(TVList tvList, int columnIndex, BitMap bitMap, int start, int end, int pos) {
      tvList.putValues(values[columnIndex], bitMap, start, end, pos, rowLength);
    }

    @Override
    public void putTo(
        AlignedTVList tvList,
        List<Integer> columnIndices,
        BitMap[] bitMaps,
        int start,
        int end,
        TSStatus[] results,
        int pos) {
      tvList.putAlignedValues(values, columnIndices, bitMaps, start, end, results, pos);
    }

    @Override
    public Object[] toTwoDArray() {
      return values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TwoDArrayValueView that = (TwoDArrayValueView) o;
      if (rowLength != that.rowLength) {
        return false;
      }
      if (!Arrays.equals(dataTypes(), that.dataTypes())) {
        return false;
      }
      for (int i = 0; i < colLength(); i++) {
        for (int j = 0; j < rowLength(); j++) {
          if (!Objects.equals(get(j, i), that.get(j, i))) {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataTypes, rowLength);
    }
  }

  @SuppressWarnings("SuspiciousSystemArraycopy")
  public static class ThreeDArrayValueView implements ValueView {

    private final Supplier<TSDataType[]> dataTypes;
    private Object[][] values;
    private final int rowLength;
    private final int singleArraySize;

    public ThreeDArrayValueView(
        Object[][] values, Supplier<TSDataType[]> dataTypes, int rowLength, int singleArraySize) {
      this.values = values;
      this.dataTypes = dataTypes;
      this.rowLength = rowLength;
      this.singleArraySize = singleArraySize;
    }

    @Override
    public int colLength() {
      return values.length;
    }

    @Override
    public int rowLength() {
      return rowLength;
    }

    @Override
    public Object get(int rowIndex, int colIndex) {
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          return ((int[]) values[colIndex][rowIndex / singleArraySize])[rowIndex % singleArraySize];
        case INT64:
        case TIMESTAMP:
          return ((long[]) values[colIndex][rowIndex / singleArraySize])
              [rowIndex % singleArraySize];
        case FLOAT:
          return ((float[]) values[colIndex][rowIndex / singleArraySize])
              [rowIndex % singleArraySize];
        case DOUBLE:
          return ((double[]) values[colIndex][rowIndex / singleArraySize])
              [rowIndex % singleArraySize];
        case TEXT:
        case BLOB:
        case STRING:
          return ((Binary[]) values[colIndex][rowIndex / singleArraySize])
              [rowIndex % singleArraySize];
        case BOOLEAN:
          return ((boolean[]) values[colIndex][rowIndex / singleArraySize])
              [rowIndex % singleArraySize];
        case VECTOR:
        case UNKNOWN:
        default:
          throw new IllegalArgumentException(dataTypes.get()[colIndex].toString());
      }
    }

    @Override
    public void set(int rowIndex, int colIndex, Object value) {
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          ((int[]) values[colIndex][rowIndex / singleArraySize])[rowIndex % singleArraySize] =
              ((int) value);
          return;
        case INT64:
        case TIMESTAMP:
          ((long[]) values[colIndex][rowIndex / singleArraySize])[rowIndex % singleArraySize] =
              ((long) value);
          return;
        case FLOAT:
          ((float[]) values[colIndex][rowIndex / singleArraySize])[rowIndex % singleArraySize] =
              ((float) value);
          return;
        case DOUBLE:
          ((double[]) values[colIndex][rowIndex / singleArraySize])[rowIndex % singleArraySize] =
              ((double) value);
          return;
        case TEXT:
        case BLOB:
        case STRING:
          ((Binary[]) values[colIndex][rowIndex / singleArraySize])[rowIndex % singleArraySize] =
              ((Binary) value);
          return;
        case BOOLEAN:
          ((boolean[]) values[colIndex][rowIndex / singleArraySize])[rowIndex % singleArraySize] =
              ((boolean) value);
          return;
        case VECTOR:
        case UNKNOWN:
        default:
          throw new IllegalArgumentException(dataTypes.get()[colIndex].toString());
      }
    }

    @Override
    public void copyTo(
        ValueView valueView, int colIndex, int thisFrom, int targetFrom, int copyLength) {
      if (valueView instanceof ThreeDArrayValueView
          && this.singleArraySize == ((ThreeDArrayValueView) valueView).singleArraySize) {
        copyLength = Math.min(copyLength, this.rowLength() - thisFrom);
        while (copyLength > 0) {
          int singleCopyLength =
              copyOneArrayTo(
                  ((ThreeDArrayValueView) valueView), colIndex, thisFrom, targetFrom, copyLength);
          thisFrom += singleCopyLength;
          targetFrom += singleCopyLength;
          copyLength -= singleCopyLength;
        }
      } else {
        ValueView.super.copyTo(valueView, colIndex, thisFrom, targetFrom, copyLength);
      }
    }

    private int copyOneArrayTo(
        ThreeDArrayValueView target, int colIndex, int thisFrom, int targetFrom, int copyLength) {
      int srcArrayPos = thisFrom % singleArraySize;
      int srcArrayRemaining = singleArraySize - srcArrayPos;
      copyLength = Math.min(copyLength, srcArrayRemaining);
      Object srcArray = values[colIndex][thisFrom / singleArraySize];
      Object targetArray = target.values[colIndex][targetFrom / singleArraySize];
      int targetArrayPos = targetFrom % singleArraySize;
      int targetArrayRemaining = singleArraySize - targetArrayPos;

      if (copyLength > targetArrayRemaining) {
        System.arraycopy(srcArray, srcArrayPos, targetArray, targetArrayPos, targetArrayRemaining);
        Object nextTargetArray = target.values[colIndex][targetFrom / singleArraySize + 1];
        System.arraycopy(
            srcArray,
            srcArrayPos + targetArrayRemaining,
            nextTargetArray,
            0,
            copyLength - targetArrayRemaining);
      } else {
        System.arraycopy(srcArray, srcArrayPos, targetArray, targetArrayPos, copyLength);
      }
      return copyLength;
    }

    @Override
    public void serializeColumn(int colIndex, ByteBuffer buffer) {
      int rowIndex = 0;
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          for (Object o : values[colIndex]) {
            int[] intValues = (int[]) o;
            for (int intValue : intValues) {
              ReadWriteIOUtils.write(intValue, buffer);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          for (Object o : values[colIndex]) {
            long[] values = (long[]) o;
            for (long value : values) {
              ReadWriteIOUtils.write(value, buffer);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case FLOAT:
          for (Object o : values[colIndex]) {
            float[] values = (float[]) o;
            for (float value : values) {
              ReadWriteIOUtils.write(value, buffer);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case DOUBLE:
          for (Object o : values[colIndex]) {
            double[] values = (double[]) o;
            for (double value : values) {
              ReadWriteIOUtils.write(value, buffer);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case BOOLEAN:
          for (Object o : values[colIndex]) {
            boolean[] values = (boolean[]) o;
            for (boolean value : values) {
              ReadWriteIOUtils.write(BytesUtils.boolToByte(value), buffer);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          for (Object o : values[colIndex]) {
            Binary[] values = (Binary[]) o;
            for (Binary value : values) {
              if (value != null) {
                ReadWriteIOUtils.write(value, buffer);
              } else {
                ReadWriteIOUtils.write(0, buffer);
              }
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(DATATYPE_UNSUPPORTED, dataTypes.get()[colIndex]));
      }
    }

    @Override
    public void serializeColumn(int colIndex, DataOutputStream stream) throws IOException {
      int rowIndex = 0;
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          for (Object o : values[colIndex]) {
            int[] intValues = (int[]) o;
            for (int intValue : intValues) {
              ReadWriteIOUtils.write(intValue, stream);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          for (Object o : values[colIndex]) {
            long[] values = (long[]) o;
            for (long value : values) {
              ReadWriteIOUtils.write(value, stream);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case FLOAT:
          for (Object o : values[colIndex]) {
            float[] values = (float[]) o;
            for (float value : values) {
              ReadWriteIOUtils.write(value, stream);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case DOUBLE:
          for (Object o : values[colIndex]) {
            double[] values = (double[]) o;
            for (double value : values) {
              ReadWriteIOUtils.write(value, stream);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case BOOLEAN:
          for (Object o : values[colIndex]) {
            boolean[] values = (boolean[]) o;
            for (boolean value : values) {
              ReadWriteIOUtils.write(BytesUtils.boolToByte(value), stream);
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          for (Object o : values[colIndex]) {
            Binary[] values = (Binary[]) o;
            for (Binary value : values) {
              if (value != null) {
                ReadWriteIOUtils.write(value, stream);
              } else {
                ReadWriteIOUtils.write(0, stream);
              }
              rowIndex++;
              if (rowIndex == rowLength) {
                return;
              }
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(DATATYPE_UNSUPPORTED, dataTypes.get()[colIndex]));
      }
    }

    @Override
    public void serializeColumn(int colIndex, IWALByteBufferView buffer, int start, int end) {
      int serializedCnt = 0;
      Object[] column = values[colIndex];
      switch (dataTypes.get()[colIndex]) {
        case INT32:
        case DATE:
          for (int i = start / singleArraySize; i < column.length; i++) {
            Object o = column[i];
            int[] values = (int[]) o;

            int j = i == start / singleArraySize ? start % singleArraySize : 0;
            for (; j < values.length; j++) {
              int value = values[j];
              serializedCnt++;
              buffer.putInt(value);
              if (serializedCnt == end - start) {
                return;
              }
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          for (int i = start / singleArraySize; i < column.length; i++) {
            Object o = column[i];
            long[] values = (long[]) o;

            int j = i == start / singleArraySize ? start % singleArraySize : 0;
            for (; j < values.length; j++) {
              long value = values[j];
              serializedCnt++;
              buffer.putLong(value);
              if (serializedCnt == end - start) {
                return;
              }
            }
          }
          break;
        case FLOAT:
          for (int i = start / singleArraySize; i < column.length; i++) {
            Object o = column[i];
            float[] values = (float[]) o;

            int j = i == start / singleArraySize ? start % singleArraySize : 0;
            for (; j < values.length; j++) {
              float value = values[j];
              serializedCnt++;
              buffer.putFloat(value);
              if (serializedCnt == end - start) {
                return;
              }
            }
          }
          break;
        case DOUBLE:
          for (int i = start / singleArraySize; i < column.length; i++) {
            Object o = column[i];
            double[] values = (double[]) o;

            int j = i == start / singleArraySize ? start % singleArraySize : 0;
            for (; j < values.length; j++) {
              double value = values[j];
              serializedCnt++;
              buffer.putDouble(value);
              if (serializedCnt == end - start) {
                return;
              }
            }
          }
          break;
        case BOOLEAN:
          for (int i = start / singleArraySize; i < column.length; i++) {
            Object o = column[i];
            boolean[] values = (boolean[]) o;

            int j = i == start / singleArraySize ? start % singleArraySize : 0;
            for (; j < values.length; j++) {
              boolean value = values[j];
              serializedCnt++;
              buffer.put(BytesUtils.boolToByte(value));
              if (serializedCnt == end - start) {
                return;
              }
            }
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          for (int i = start / singleArraySize; i < column.length; i++) {
            Object o = column[i];
            Binary[] values = (Binary[]) o;

            int j = i == start / singleArraySize ? start % singleArraySize : 0;
            for (; j < values.length; j++) {
              Binary value = values[j];
              serializedCnt++;
              if (value != null) {
                buffer.putInt(value.getLength());
                buffer.put(value.getValues());
              } else {
                buffer.putInt(0);
              }
              if (serializedCnt == end - start) {
                return;
              }
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(DATATYPE_UNSUPPORTED, dataTypes.get()[colIndex]));
      }
    }

    @Override
    public TSDataType[] dataTypes() {
      return dataTypes.get();
    }

    @Override
    public void castTo(int colIndex, TSDataType newType) {
      for (int i = 0; i < values[colIndex].length; i++) {
        Object originalArray = values[colIndex][i];
        values[colIndex][i] = newType.castFromArray(dataTypes.get()[colIndex], originalArray);
        if (originalArray != values[colIndex][i]) {
          release(originalArray);
        }
      }
    }

    @Override
    public void insertColumn(int pos, ColumnSchema columnSchema) {
      Object[][] tmpColumns = new Object[values.length + 1][];
      System.arraycopy(values, 0, tmpColumns, 0, pos);
      int arrayNum = rowLength / singleArraySize + rowLength % singleArraySize == 0 ? 0 : 1;
      tmpColumns[pos] = new Object[arrayNum];
      for (int i = 0; i < arrayNum; i++) {
        tmpColumns[pos][i] =
            PrimitiveArrayManager.allocate(
                InternalTypeManager.getTSDataType(columnSchema.getType()));
      }
      System.arraycopy(values, pos, tmpColumns, pos + 1, values.length - pos);
      values = tmpColumns;
    }

    private void release(Object array) {
      PrimitiveArrayManager.release(array);
    }

    @Override
    public void swapColumn(int src, int target) {
      CommonUtils.swapArray(values, src, target);
    }

    @Override
    public long ramSize(MeasurementSchema[] measurementSchemas) {
      return InsertNodeMemoryEstimator.sizeOfColumns(values, measurementSchemas, null);
    }

    @Override
    public void reserveColumns(List<Integer> columnsToReserve) {
      columnsToReserve.sort(null);
      int reserveColumnCursor = 0;
      for (int i = 0; i < values.length; i++) {
        if (reserveColumnCursor < columnsToReserve.size()
            && i != columnsToReserve.get(reserveColumnCursor)) {
          // i is a column to remove
          Object[] value = values[i];
          for (int j = 0, valueLength = value.length; j < valueLength; j++) {
            Object array = value[j];
            release(array);
            value[j] = null;
          }
        } else {
          // i is a column to reserve, copy it later
          reserveColumnCursor++;
        }
      }

      Object[][] tmpValue = new Object[columnsToReserve.size()][];
      for (int j = 0, columnsToReserveSize = columnsToReserve.size();
          j < columnsToReserveSize;
          j++) {
        Integer realPos = columnsToReserve.get(j);
        tmpValue[j] = values[realPos];
      }
      values = tmpValue;
    }

    @Override
    public void release() {
      for (Object[] column : values) {
        for (int i = 0, columnLength = column.length; i < columnLength; i++) {
          Object array = column[i];
          release(array);
          column[i] = null;
        }
      }
    }

    @Override
    public void putTo(TVList tvList, int columnIndex, BitMap bitMap, int start, int end, int pos) {
      if (end > rowLength) {
        end = rowLength;
      }

      int current = start;
      while (current < end) {
        // put one array to TVList
        int arrayIndex = current / singleArraySize;
        int arrayRemaining = singleArraySize - current % singleArraySize;
        int copyLength = Math.min(arrayRemaining, end - current);

        int arrayStart = current % singleArraySize;
        int arrayEnd = arrayStart + copyLength;
        tvList.putValues(
            values[columnIndex][arrayIndex],
            bitMap == null ? null : bitMap.getRegion(current, current + copyLength),
            arrayStart,
            arrayEnd,
            pos,
            singleArraySize);

        current += copyLength;
      }
    }

    @Override
    public void putTo(
        AlignedTVList tvList,
        List<Integer> columnIndices,
        BitMap[] bitMaps,
        int tabletStart,
        int tabletEnd,
        TSStatus[] results,
        int tvListPos) {
      if (tabletEnd > rowLength) {
        tabletEnd = rowLength;
      }

      tvList.resetColumnInsertedMap();
      for (int i = 0; i < columnIndices.size(); i++) {
        int currentTabletPos = tabletStart;
        int currentTVListPos = tvListPos;
        while (currentTabletPos < tabletEnd) {
          // put one array to TVList
          int arrayIndex = currentTabletPos / singleArraySize;
          int arrayRemaining = singleArraySize - currentTabletPos % singleArraySize;
          int copyLength = Math.min(arrayRemaining, tabletEnd - currentTabletPos);

          int arrayStart = currentTabletPos % singleArraySize;
          int arrayEnd = arrayStart + copyLength;
          int resultOffset = currentTabletPos - arrayStart;
          tvList.putAlignedValues(
              values[i][arrayIndex],
              columnIndices.get(i),
              bitMaps[i] == null
                  ? null
                  : bitMaps[i].getRegion(currentTabletPos, currentTabletPos + copyLength),
              arrayStart,
              arrayEnd,
              resultOffset,
              results,
              currentTVListPos);

          currentTabletPos += copyLength;
          currentTVListPos += copyLength;
        }
      }
      tvList.markNotInsertedColumns(tabletStart, tabletEnd);
      LOGGER.info(
          "Put {} values into list {}", tabletEnd - tabletStart, System.identityHashCode(tvList));
    }

    @Override
    public Object[] toTwoDArray() {
      Object[] twoDArray = new Object[values.length];
      for (int i = 0; i < values.length; i++) {
        if (dataTypes.get()[i] == null) {
          continue;
        }
        switch (dataTypes.get()[i]) {
          case INT32:
          case DATE:
            twoDArray[i] = new int[rowLength];
            break;
          case INT64:
          case TIMESTAMP:
            twoDArray[i] = new Long[rowLength];
            break;
          case FLOAT:
            twoDArray[i] = new Float[rowLength];
            break;
          case DOUBLE:
            twoDArray[i] = new double[rowLength];
            break;
          case STRING:
          case BLOB:
          case TEXT:
            twoDArray[i] = new Binary[rowLength];
            break;
          case BOOLEAN:
            twoDArray[i] = new Boolean[rowLength];
            break;
          case UNKNOWN:
          case VECTOR:
          default:
            throw new UnSupportedDataTypeException(dataTypes.get()[i].toString());
        }
        int arrayIndex = 0;
        for (; arrayIndex < values[i].length - 1; arrayIndex++) {
          System.arraycopy(
              values[i][arrayIndex],
              0,
              twoDArray[i],
              arrayIndex * singleArraySize,
              singleArraySize);
        }
        System.arraycopy(
            values[i][arrayIndex],
            0,
            twoDArray[i],
            arrayIndex * singleArraySize,
            rowLength % singleArraySize);
      }
      return twoDArray;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ThreeDArrayValueView that = (ThreeDArrayValueView) o;
      if (rowLength != that.rowLength) {
        return false;
      }
      if (!Arrays.equals(dataTypes(), that.dataTypes())) {
        return false;
      }
      for (int i = 0; i < colLength(); i++) {
        for (int j = 0; j < rowLength(); j++) {
          if (!Objects.equals(get(j, i), that.get(j, i))) {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataTypes, rowLength, singleArraySize);
    }
  }

  public static class ColumnMappedValueView implements ValueView {
    private final ValueView innerValue;
    private List<Integer> realIndexes;

    public ColumnMappedValueView(ValueView innerValue, List<Integer> realIndexes) {
      this.innerValue = innerValue;
      this.realIndexes = realIndexes;
    }

    @Override
    public void serializeColumn(int colIndex, ByteBuffer buffer) {
      innerValue.serializeColumn(realIndexes.get(colIndex), buffer);
    }

    @Override
    public void serializeColumn(int colIndex, DataOutputStream stream) throws IOException {
      innerValue.serializeColumn(realIndexes.get(colIndex), stream);
    }

    @Override
    public void serializeColumn(int colIndex, IWALByteBufferView buffer, int start, int end) {
      innerValue.serializeColumn(realIndexes.get(colIndex), buffer, start, end);
    }

    @Override
    public void castTo(int colIndex, TSDataType newType) {
      synchronized (innerValue) {
        innerValue.castTo(realIndexes.get(colIndex), newType);
      }
    }

    @Override
    public long getColumnSize(int colIndex, int start, int end) {
      return innerValue.getColumnSize(realIndexes.get(colIndex), start, end);
    }

    @Override
    public void copyTo(
        ValueView valueView, int colIndex, int thisFrom, int targetFrom, int copyLength) {
      innerValue.copyTo(valueView, colIndex, thisFrom, targetFrom, copyLength);
    }

    @Override
    public TSDataType[] dataTypes() {
      return innerValue.dataTypes();
    }

    @Override
    public void set(int rowIndex, int colIndex, Object value) {
      innerValue.set(rowIndex, realIndexes.get(colIndex), value);
    }

    @Override
    public Object get(int rowIndex, int colIndex) {
      return innerValue.get(rowIndex, realIndexes.get(colIndex));
    }

    @Override
    public int rowLength() {
      return innerValue.rowLength();
    }

    @Override
    public int colLength() {
      return realIndexes.size();
    }

    @Override
    public void insertColumn(int pos, ColumnSchema columnSchema) {
      throw new UnsupportedOperationException("Mapped view cannot be inserted");
    }

    @Override
    public void swapColumn(int src, int target) {
      Collections.swap(realIndexes, src, target);
    }

    @Override
    public long ramSize(MeasurementSchema[] measurementSchemas) {
      if (innerValue instanceof TwoDArrayValueView) {
        return InsertNodeMemoryEstimator.sizeOfColumns(
            ((TwoDArrayValueView) innerValue).values, measurementSchemas, realIndexes);
      } else if (innerValue instanceof ThreeDArrayValueView) {
        return InsertNodeMemoryEstimator.sizeOfColumns(
            ((ThreeDArrayValueView) innerValue).values, measurementSchemas, realIndexes);
      } else {
        return innerValue.ramSize(measurementSchemas);
      }
    }

    @Override
    public void reserveColumns(List<Integer> columnsToReserve) {
      realIndexes = columnsToReserve.stream().map(realIndexes::get).collect(Collectors.toList());
    }

    @Override
    public void release() {
      // do nothing
    }

    @Override
    public void putTo(TVList tvList, int columnIndex, BitMap bitMap, int start, int end, int pos) {
      innerValue.putTo(tvList, realIndexes.get(columnIndex), bitMap, start, end, pos);
    }

    @Override
    public void putTo(
        AlignedTVList tvList,
        List<Integer> tvListColumnIndices,
        BitMap[] bitMaps,
        int start,
        int end,
        TSStatus[] results,
        int pos) {
      List<Integer> columnIndicesForInner = new ArrayList<>(innerValue.colLength());
      // put tvListColumnIndices into the associated places for the inner columns
      // if tvListColumnIndices = [0, 1, 2], realIndices = [1, 3, 4], innerValue.colLength = 5
      // then columnIndicesForInner = [-1, 0, -1, 1, 2]
      for (int i = 0; i < innerValue.colLength(); i++) {
        columnIndicesForInner.add(-1);
      }
      for (int i = 0; i < tvListColumnIndices.size(); i++) {
        columnIndicesForInner.set(realIndexes.get(i), tvListColumnIndices.get(i));
      }
      // similarly for bitmaps
      BitMap[] bitmapsForInner = new BitMap[innerValue.colLength()];
      for (int i = 0, realIndexesSize = realIndexes.size(); i < realIndexesSize; i++) {
        Integer realIndex = realIndexes.get(i);
        bitmapsForInner[realIndex] = bitMaps[i];
      }

      innerValue.putTo(tvList, columnIndicesForInner, bitmapsForInner, start, end, results, pos);
    }

    @Override
    public Object[] toTwoDArray() {
      Object[] twoDArrayInner = innerValue.toTwoDArray();
      Object[] twoDArray = new Object[realIndexes.size()];
      for (int i = 0; i < realIndexes.size(); i++) {
        twoDArray[i] = twoDArrayInner[realIndexes.get(i)];
      }

      return twoDArray;
    }
  }
}
