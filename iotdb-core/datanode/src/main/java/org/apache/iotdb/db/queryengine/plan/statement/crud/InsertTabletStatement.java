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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertTablet;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.BitMapUtils;
import org.apache.iotdb.db.utils.CommonUtils;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InsertTabletStatement extends InsertBaseStatement implements ISchemaValidation {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTabletStatement.class);

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InsertTabletStatement.class);

  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  /**
   * Get the instance size of InsertTabletStatement for memory calculation.
   *
   * @return instance size in bytes
   */
  public static long getInstanceSize() {
    return INSTANCE_SIZE;
  }

  protected long[] times; // times should be sorted. It is done in the session API.
  protected BitMap[] nullBitMaps;
  protected Object[] columns;

  protected IDeviceID[] deviceIDs;

  protected boolean singleDevice;

  protected int rowCount = 0;

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

  public InsertTabletStatement(
      final Tablet tablet, final boolean isAligned, final String databaseName)
      throws MetadataException {
    this();
    setMeasurements(
        tablet.getSchemas().stream()
            .map(IMeasurementSchema::getMeasurementName)
            .toArray(String[]::new));
    setDataTypes(
        tablet.getSchemas().stream().map(IMeasurementSchema::getType).toArray(TSDataType[]::new));
    if (Objects.nonNull(databaseName)) {
      setDevicePath(new PartialPath(tablet.getTableName(), false));
    } else {
      setDevicePath(DataNodeDevicePathCache.getInstance().getPartialPath(tablet.getDeviceId()));
    }
    setAligned(isAligned);
    setTimes(tablet.getTimestamps());
    setRowCount(tablet.getRowSize());
    final Object[] columns = new Object[tablet.getValues().length];
    for (int i = 0; i < tablet.getValues().length; ++i) {
      columns[i] = convertTableColumn(tablet.getValues()[i], tablet.getRowSize(), dataTypes[i]);
    }
    setColumns(columns);
    setBitMaps(tablet.getBitMaps());

    if (Objects.nonNull(databaseName)) {
      setWriteToTable(true);
      setDatabaseName(databaseName);
      setColumnCategories(
          tablet.getColumnTypes().stream()
              .map(TsTableColumnCategory::fromTsFileColumnCategory)
              .toArray(TsTableColumnCategory[]::new));
    }
  }

  private Object convertTableColumn(final Object input, final int rowCount, final TSDataType type) {
    if (input instanceof LocalDate[]) {
      return Arrays.stream(((LocalDate[]) input))
          .map(date -> Objects.nonNull(date) ? DateUtils.parseDateExpressionToInt(date) : 0)
          .mapToInt(Integer::intValue)
          .toArray();
    } else if (input instanceof Binary[]) {
      return Arrays.stream(((Binary[]) input))
          .map(binary -> Objects.nonNull(binary) ? binary : Binary.EMPTY_VALUE)
          .toArray(Binary[]::new);
    } else if (input == null) {
      switch (type) {
        case BOOLEAN:
          return new boolean[rowCount];
        case INT32:
        case DATE:
          return new int[rowCount];
        case INT64:
        case TIMESTAMP:
          return new long[rowCount];
        case FLOAT:
          return new float[rowCount];
        case DOUBLE:
          return new double[rowCount];
        case TEXT:
        case BLOB:
        case STRING:
          final Binary[] result = new Binary[rowCount];
          Arrays.fill(result, Binary.EMPTY_VALUE);
          return result;
        default:
          throw new UnSupportedDataTypeException(
              String.format("data type %s is not supported when convert data at client", type));
      }
    }

    return input;
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

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns(Object[] columns) {
    this.columns = columns;
    deviceIDs = null;
  }

  public BitMap[] getBitMaps() {
    return nullBitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.nullBitMaps = bitMaps;
    deviceIDs = null;
  }

  @Override
  public void setColumnCategories(TsTableColumnCategory[] columnCategories) {
    super.setColumnCategories(columnCategories);
    deviceIDs = null;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  @Override
  public boolean isEmpty() {
    return rowCount == 0
        || times.length == 0
        || measurements.length == 0
        || dataTypes.length == 0
        || columns.length == 0;
  }

  public List<TTimePartitionSlot> getTimePartitionSlots() {
    List<TTimePartitionSlot> result = new ArrayList<>();
    long upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times[0]);
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[0]);
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= upperBoundOfTimePartition) {
        result.add(timePartitionSlot);
        // next init
        upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times[i]);
        timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[i]);
      }
    }
    result.add(timePartitionSlot);
    return result;
  }

  public TTimePartitionSlot getTimePartitionSlot(int i) {
    return TimePartitionUtils.getTimePartitionSlot(times[i]);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertTablet(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    if (measurements == null) {
      return ret;
    }
    for (String m : measurements) {
      if (m == null) {
        continue;
      }
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
    if (dataTypes == null
        || columns == null
        || columnIndex < 0
        || columnIndex >= dataTypes.length
        || columnIndex >= columns.length
        || dataTypes[columnIndex] == null
        || columns[columnIndex] == null) {
      return false;
    }
    if (dataType.isCompatible(dataTypes[columnIndex])) {
      columns[columnIndex] = dataType.castFromArray(dataTypes[columnIndex], columns[columnIndex]);
      dataTypes[columnIndex] = dataType;
      return true;
    }
    return false;
  }

  @Override
  public void markFailedMeasurement(int index, Exception cause) {
    if (measurements == null
        || index < 0
        || index >= measurements.length
        || measurements[index] == null) {
      return;
    }

    if (failedMeasurementIndex2Info == null) {
      failedMeasurementIndex2Info = new HashMap<>();
    }

    InsertBaseStatement.FailedMeasurementInfo failedMeasurementInfo =
        new InsertBaseStatement.FailedMeasurementInfo(
            measurements[index],
            dataTypes != null && index < dataTypes.length ? dataTypes[index] : null,
            columns != null && index < columns.length ? columns[index] : null,
            cause);
    failedMeasurementIndex2Info.putIfAbsent(index, failedMeasurementInfo);

    measurements[index] = null;
    if (dataTypes != null && index < dataTypes.length) {
      dataTypes[index] = null;
    }
    if (columns != null && index < columns.length) {
      columns[index] = null;
    }
  }

  @Override
  public void removeAllFailedMeasurementMarks() {
    if (failedMeasurementIndex2Info == null) {
      return;
    }
    failedMeasurementIndex2Info.forEach(
        (index, info) -> {
          if (measurements != null && index < measurements.length) {
            measurements[index] = info.getMeasurement();
          }
          if (dataTypes != null && index < dataTypes.length) {
            dataTypes[index] = info.getDataType();
          }
          if (columns != null && index < columns.length) {
            columns[index] = info.getValue();
          }
        });
    failedMeasurementIndex2Info.clear();
  }

  @Override
  public void semanticCheck() {
    super.semanticCheck();
    if (measurements.length != columns.length) {
      throw new SemanticException(
          String.format(
              "the measurementList's size %d is not consistent with the columnList's size %d",
              measurements.length, columns.length));
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
      Object[] copiedColumns = new Object[pairList.size()];
      String[] measurements = new String[pairList.size()];
      BitMap[] copiedBitMaps = new BitMap[pairList.size()];
      MeasurementSchema[] measurementSchemas = new MeasurementSchema[pairList.size()];
      TSDataType[] dataTypes = new TSDataType[pairList.size()];
      for (int i = 0; i < pairList.size(); i++) {
        int realIndex = pairList.get(i).right;
        copiedColumns[i] =
            this.columns != null && realIndex < this.columns.length
                ? this.columns[realIndex]
                : null;
        measurements[i] =
            Objects.nonNull(this.measurements[realIndex]) ? pairList.get(i).left : null;
        measurementSchemas[i] =
            this.measurementSchemas != null && realIndex < this.measurementSchemas.length
                ? this.measurementSchemas[realIndex]
                : null;
        dataTypes[i] =
            this.dataTypes != null && realIndex < this.dataTypes.length
                ? this.dataTypes[realIndex]
                : null;
        if (this.nullBitMaps != null && realIndex < this.nullBitMaps.length) {
          copiedBitMaps[i] = this.nullBitMaps[realIndex];
        }
        if (this.measurementIsAligned != null && realIndex < this.measurementIsAligned.length) {
          statement.setAligned(this.measurementIsAligned[realIndex]);
        }
      }
      statement.setColumns(copiedColumns);
      statement.setMeasurements(measurements);
      statement.setMeasurementSchemas(measurementSchemas);
      statement.setDataTypes(dataTypes);
      if (this.nullBitMaps != null) {
        statement.setBitMaps(BitMapUtils.compactBitMaps(copiedBitMaps, rowCount));
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
    return times[0];
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    if (dataTypes == null
        || columns == null
        || index < 0
        || index >= dataTypes.length
        || index >= columns.length
        || dataTypes[index] == null
        || columns[index] == null) {
      return null;
    }
    Object value;
    switch (dataTypes[index]) {
      case INT32:
      case DATE:
        int[] intValues = (int[]) columns[index];
        value = intValues[0];
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) columns[index];
        value = longValues[0];
        break;
      case FLOAT:
        float[] floatValues = (float[]) columns[index];
        value = floatValues[0];
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) columns[index];
        value = doubleValues[0];
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) columns[index];
        value = boolValues[0];
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) columns[index];
        value = binaryValues[0];
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataTypes[index]));
    }
    return value;
  }

  @Override
  public boolean isColumnPresent(final int index) {
    return super.isColumnPresent(index)
        && columns != null
        && index < columns.length
        && columns[index] != null;
  }

  @Override
  public TSDataType getDataType(int index) {
    return dataTypes != null && index >= 0 && index < dataTypes.length ? dataTypes[index] : null;
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
    } else if (index >= measurementSchemas.length) {
      measurementSchemas = Arrays.copyOf(measurementSchemas, measurements.length);
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
    } else if (index >= this.measurementIsAligned.length) {
      this.measurementIsAligned =
          Arrays.copyOf(this.measurementIsAligned, this.measurements.length);
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
      String[] deviceIdSegments = new String[getTagColumnIndices().size() + 1];
      deviceIdSegments[0] = this.getTableName();
      for (int i = 0; i < getTagColumnIndices().size(); i++) {
        final Integer columnIndex = getTagColumnIndices().get(i);
        final Object idSegment =
            isNull(rowIdx, columnIndex) ? null : getColumnValue(rowIdx, columnIndex);
        deviceIdSegments[i + 1] = idSegment != null ? idSegment.toString() : null;
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

    Object[] tmpColumns = new Object[measurements.length];
    copyWithInsertedSlot(columns, tmpColumns, pos);
    tmpColumns[pos] =
        CommonUtils.createValueColumnOfDataType(
            InternalTypeManager.getTSDataType(columnSchema.getType()),
            columnSchema.getColumnCategory(),
            rowCount);
    columns = tmpColumns;

    deviceIDs = null;
  }

  @Override
  public void swapColumn(int src, int target) {
    super.swapColumn(src, target);
    if (nullBitMaps != null) {
      if (nullBitMaps.length < measurements.length) {
        nullBitMaps = Arrays.copyOf(nullBitMaps, measurements.length);
      }
      CommonUtils.swapArray(nullBitMaps, src, target);
    }
    if (columns == null) {
      columns = new Object[measurements.length];
    } else if (columns.length < measurements.length) {
      columns = Arrays.copyOf(columns, measurements.length);
    }
    CommonUtils.swapArray(columns, src, target);
    deviceIDs = null;
  }

  @Override
  public void rebuildArraysAfterExpansion(
      final int[] newToOldMapping, final String[] newMeasurements) {
    final int newLength = newToOldMapping.length;

    // Call parent to rebuild base arrays
    super.rebuildArraysAfterExpansion(newToOldMapping, newMeasurements);

    // Save old arrays
    final BitMap[] oldNullBitMaps = nullBitMaps;
    final Object[] oldColumns = columns;
    final boolean[] oldMeasurementIsAligned = measurementIsAligned;

    // Create new arrays
    final BitMap[] newNullBitMaps = new BitMap[newLength];
    final Object[] newColumns = oldColumns != null ? new Object[newLength] : null;
    final boolean[] newMeasurementIsAligned =
        oldMeasurementIsAligned != null ? new boolean[newLength] : null;

    // Rebuild arrays using mapping: newToOldMapping[newIdx] = oldIdx
    // If oldIdx == -1, it's a missing TAG column, fill with default values
    for (int newIdx = 0; newIdx < newLength; newIdx++) {
      final int oldIdx = newToOldMapping[newIdx];
      if (oldIdx == -1) {
        // Create new BitMap with all positions marked (all null)
        newNullBitMaps[newIdx] = new BitMap(rowCount);
        newNullBitMaps[newIdx].markAll();
        if (newColumns != null) {
          // Create default column based on data type (STRING for TAG)
          newColumns[newIdx] =
              CommonUtils.createValueColumnOfDataType(
                  TSDataType.STRING, TsTableColumnCategory.TAG, rowCount);
        }
        if (newMeasurementIsAligned != null) {
          // Default to false for missing TAG columns
          newMeasurementIsAligned[newIdx] = false;
        }
      } else {
        // Copy from old array
        if (oldNullBitMaps != null && oldIdx < oldNullBitMaps.length) {
          newNullBitMaps[newIdx] = oldNullBitMaps[oldIdx];
        }
        if (newColumns != null && oldColumns != null && oldIdx < oldColumns.length) {
          newColumns[newIdx] = oldColumns[oldIdx];
        }
        if (newMeasurementIsAligned != null && oldMeasurementIsAligned != null) {
          newMeasurementIsAligned[newIdx] =
              oldIdx < oldMeasurementIsAligned.length && oldMeasurementIsAligned[oldIdx];
        }
      }
    }

    // Replace old arrays with new arrays
    nullBitMaps = newNullBitMaps;
    columns = newColumns;
    measurementIsAligned = newMeasurementIsAligned;

    deviceIDs = null;
  }

  @Override
  protected long calculateBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOf(times)
        + RamUsageEstimator.sizeOf(nullBitMaps)
        + InsertNodeMemoryEstimator.sizeOfColumns(columns, measurementSchemas)
        + (Objects.nonNull(deviceIDs)
            ? Arrays.stream(deviceIDs)
                .mapToLong(InsertNodeMemoryEstimator::sizeOfIDeviceID)
                .reduce(0L, Long::sum)
            : 0L);
  }

  public boolean isNull(int row, int col) {
    return nullBitMaps != null
        && row >= 0
        && col >= 0
        && col < nullBitMaps.length
        && nullBitMaps[col] != null
        && nullBitMaps[col].isMarked(row);
  }

  private Object getColumnValue(final int rowIdx, final int columnIndex) {
    if (columns == null
        || columnIndex < 0
        || columnIndex >= columns.length
        || columns[columnIndex] == null
        || !(columns[columnIndex] instanceof Object[])) {
      return null;
    }
    final Object[] values = (Object[]) columns[columnIndex];
    return rowIdx >= 0 && rowIdx < values.length ? values[rowIdx] : null;
  }

  @Override
  protected void subRemoveAttributeColumns(List<Integer> columnsToKeep) {
    if (columns != null) {
      columns =
          columnsToKeep.stream().filter(i -> i < columns.length).map(i -> columns[i]).toArray();
    }
    if (nullBitMaps != null) {
      nullBitMaps =
          columnsToKeep.stream()
              .filter(i -> i < nullBitMaps.length)
              .map(i -> nullBitMaps[i])
              .toArray(BitMap[]::new);
    }
    deviceIDs = null;
  }

  /**
   * Convert this InsertTabletStatement to Tablet. This method constructs a Tablet object from this
   * statement, converting all necessary fields. All arrays are copied to rowSize length to ensure
   * immutability.
   *
   * @return Tablet object
   * @throws MetadataException if conversion fails
   */
  public Tablet convertToTablet() throws MetadataException {
    try {
      // Get deviceId/tableName from devicePath
      final String deviceIdOrTableName =
          this.getDevicePath() != null ? this.getDevicePath().getFullPath() : "";

      // Get schemas from measurementSchemas
      final MeasurementSchema[] measurementSchemas = this.getMeasurementSchemas();
      final String[] measurements = this.getMeasurements();
      final TSDataType[] dataTypes = this.getDataTypes();
      // If measurements and dataTypes are not null, use measurements.length as the standard length
      final int originalSchemaSize = measurements != null ? measurements.length : 0;

      // Build schemas and track valid column indices (skip null columns)
      // measurements and dataTypes being null is standard - skip those columns
      final List<IMeasurementSchema> schemas = new ArrayList<>(originalSchemaSize);
      final int[] validColumnIndices = new int[originalSchemaSize];
      int validColumnCount = 0;
      final Object[] statementColumns = this.getColumns();
      if (dataTypes != null && statementColumns != null) {
        final int dataTypeSize = Math.min(originalSchemaSize, dataTypes.length);
        for (int i = 0; i < dataTypeSize; i++) {
          if (measurements[i] != null
              && dataTypes[i] != null
              && i < statementColumns.length
              && statementColumns[i] != null) {
            final MeasurementSchema measurementSchema =
                measurementSchemas != null && i < measurementSchemas.length
                    ? measurementSchemas[i]
                    : null;
            schemas.add(
                measurementSchema != null
                        && Objects.equals(measurementSchema.getMeasurementName(), measurements[i])
                        && measurementSchema.getType() == dataTypes[i]
                    ? measurementSchema
                    : new MeasurementSchema(measurements[i], dataTypes[i]));
            validColumnIndices[validColumnCount++] = i;
          }
          // Skip null columns - don't add to schemas or validColumnIndices
        }
      }

      final int schemaSize = validColumnCount;

      // Get columnTypes (for table model) - only for valid columns
      final TsTableColumnCategory[] columnCategories = this.getColumnCategories();
      final List<ColumnCategory> tabletColumnTypes = new ArrayList<>(schemaSize);
      if (columnCategories != null && columnCategories.length > 0) {
        for (int i = 0; i < schemaSize; i++) {
          final int validIndex = validColumnIndices[i];
          if (validIndex < columnCategories.length && columnCategories[validIndex] != null) {
            tabletColumnTypes.add(columnCategories[validIndex].toTsFileColumnType());
          } else {
            tabletColumnTypes.add(ColumnCategory.FIELD);
          }
        }
      } else {
        // Default to FIELD for all valid columns if not specified
        for (int i = 0; i < schemaSize; i++) {
          tabletColumnTypes.add(ColumnCategory.FIELD);
        }
      }

      // Get timestamps - always copy to ensure immutability
      final long[] times = this.getTimes();
      final int rowSize = this.getRowCount();
      final long[] timestamps;
      if (rowSize == 0) {
        timestamps = new long[0];
      } else if (times != null && times.length >= rowSize) {
        timestamps = Arrays.copyOf(times, rowSize);
      } else {
        LOGGER.warn(
            "Times array is null or too small. times.length={}, rowSize={}, deviceId={}",
            times != null ? times.length : 0,
            rowSize,
            deviceIdOrTableName);
        timestamps = new long[0];
      }

      // Get values - convert Statement columns to Tablet format, only for valid columns
      // All arrays are copied to rowSize length
      final Object[] tabletValues = new Object[schemaSize];
      if (statementColumns != null && statementColumns.length > 0) {
        for (int i = 0; i < schemaSize; i++) {
          final int originalIndex = validColumnIndices[i];
          if (originalIndex < statementColumns.length
              && statementColumns[originalIndex] != null
              && dataTypes[originalIndex] != null) {
            tabletValues[i] =
                convertColumnToTablet(
                    statementColumns[originalIndex], dataTypes[originalIndex], rowSize);
          } else {
            tabletValues[i] = null;
          }
        }
      }

      // Get bitMaps - copy and truncate to rowSize, only for valid columns
      final BitMap[] originalBitMaps = this.getBitMaps();
      BitMap[] bitMaps = null;
      if (originalBitMaps != null && originalBitMaps.length > 0) {
        final BitMap[] copiedBitMaps = new BitMap[schemaSize];
        boolean hasMarkedBitMap = false;
        for (int i = 0; i < schemaSize; i++) {
          final int originalIndex = validColumnIndices[i];
          if (originalIndex < originalBitMaps.length && originalBitMaps[originalIndex] != null) {
            final BitMap originalBitMap = originalBitMaps[originalIndex];
            if (!originalBitMap.isAllUnmarked(Math.min(rowSize, originalBitMap.getSize()))) {
              copiedBitMaps[i] = new BitMap(rowSize, originalBitMap.getTruncatedByteArray(rowSize));
              hasMarkedBitMap = true;
            }
          } else {
            copiedBitMaps[i] = null;
          }
        }
        if (hasMarkedBitMap) {
          bitMaps = copiedBitMaps;
        }
      }

      // Create Tablet using the full constructor
      // Tablet(String tableName, List<IMeasurementSchema> schemas, List<ColumnCategory>
      // columnTypes,
      //        long[] timestamps, Object[] values, BitMap[] bitMaps, int rowSize)
      return new Tablet(
          deviceIdOrTableName,
          schemas,
          tabletColumnTypes,
          timestamps,
          tabletValues,
          bitMaps,
          rowSize);
    } catch (final Exception e) {
      throw new MetadataException(
          DataNodeQueryMessages.FAILED_TO_CONVERT_INSERTTABLETSTATEMENT_TO_TABLET, e);
    }
  }

  /**
   * Convert a single column value from Statement format to Tablet format. Statement uses primitive
   * arrays (e.g., int[], long[], float[]), while Tablet may need different format. All arrays are
   * copied to rowSize length to ensure immutability - even if the original array is modified, the
   * converted array remains unchanged.
   *
   * @param columnValue column value from Statement (primitive array)
   * @param dataType data type of the column
   * @param rowSize number of rows to copy
   * @return column value in Tablet format (copied to rowSize)
   */
  private Object convertColumnToTablet(
      final Object columnValue, final TSDataType dataType, final int rowSize) {

    if (columnValue == null) {
      return null;
    }

    if (TSDataType.DATE.equals(dataType)) {
      final int[] values = (int[]) columnValue;
      final LocalDate[] localDateValue = new LocalDate[rowSize];
      final int size = Math.min(values.length, rowSize);
      for (int i = 0; i < size; i++) {
        localDateValue[i] = DateUtils.parseIntToLocalDate(values[i]);
      }
      return localDateValue;
    }

    // For primitive arrays, copy to rowSize
    if (columnValue instanceof boolean[]) {
      final boolean[] original = (boolean[]) columnValue;
      return Arrays.copyOf(original, rowSize);
    } else if (columnValue instanceof int[]) {
      final int[] original = (int[]) columnValue;
      return Arrays.copyOf(original, rowSize);
    } else if (columnValue instanceof long[]) {
      final long[] original = (long[]) columnValue;
      return Arrays.copyOf(original, rowSize);
    } else if (columnValue instanceof float[]) {
      final float[] original = (float[]) columnValue;
      return Arrays.copyOf(original, rowSize);
    } else if (columnValue instanceof double[]) {
      final double[] original = (double[]) columnValue;
      return Arrays.copyOf(original, rowSize);
    } else if (columnValue instanceof Binary[]) {
      // For Binary arrays, create a new array and copy references to rowSize
      final Binary[] original = (Binary[]) columnValue;
      return Arrays.copyOf(original, rowSize);
    }

    // For other types, return as-is (should not happen for standard types)
    return columnValue;
  }

  @Override
  public String toString() {
    final int size = CommonDescriptor.getInstance().getConfig().getPathLogMaxSize();
    return "InsertTabletStatement{"
        + "deviceIDs="
        + Arrays.toString(deviceIDs)
        + ", measurements="
        + Arrays.toString(
            Objects.nonNull(measurements) && measurements.length > size
                ? Arrays.copyOf(measurements, size)
                : measurements)
        + ", rowCount="
        + rowCount
        + ", timeRange=["
        + (Objects.nonNull(times) && times.length > 0
            ? times[0] + ", " + times[times.length - 1]
            : "")
        + "]"
        + '}';
  }
}
