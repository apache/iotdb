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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.sql.SemanticException;
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
import org.apache.iotdb.db.utils.CommonUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertTabletStatement extends InsertBaseStatement implements ISchemaValidation {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTabletStatement.class);

  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  protected long[] times; // times should be sorted. It is done in the session API.
  protected BitMap[] nullBitMaps;
  protected Object[] columns;

  private IDeviceID[] deviceIDs;

  private boolean singleDevice;

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
  }

  public BitMap[] getBitMaps() {
    return nullBitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.nullBitMaps = bitMaps;
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
    if (CommonUtils.checkCanCastType(dataTypes[columnIndex], dataType)) {
      columns[columnIndex] =
          CommonUtils.castArray(dataTypes[columnIndex], dataType, columns[columnIndex]);
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
        new InsertBaseStatement.FailedMeasurementInfo(
            measurements[index], dataTypes[index], columns[index], cause);
    failedMeasurementIndex2Info.putIfAbsent(index, failedMeasurementInfo);

    measurements[index] = null;
    dataTypes[index] = null;
    columns[index] = null;
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
          columns[index] = info.getValue();
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
        copiedColumns[i] = this.columns[realIndex];
        measurements[i] = pairList.get(i).left;
        measurementSchemas[i] = this.measurementSchemas[realIndex];
        dataTypes[i] = this.dataTypes[realIndex];
        if (this.nullBitMaps != null) {
          copiedBitMaps[i] = this.nullBitMaps[realIndex];
        }
        if (this.measurementIsAligned != null) {
          statement.setAligned(this.measurementIsAligned[realIndex]);
        }
      }
      statement.setColumns(copiedColumns);
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
    return times[0];
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
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
        deviceIdSegments[i + 1] =
            isNull ? null : ((Object[]) columns[columnIndex])[rowIdx].toString();
      }
      deviceIDs[rowIdx] = Factory.DEFAULT_FACTORY.create(deviceIdSegments);
    }

    return deviceIDs[rowIdx];
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

    Object[] tmpColumns = new Object[columns.length + 1];
    System.arraycopy(columns, 0, tmpColumns, 0, pos);
    tmpColumns[pos] =
        CommonUtils.createValueColumnOfDataType(
            InternalTypeManager.getTSDataType(columnSchema.getType()),
            columnSchema.getColumnCategory(),
            rowCount);
    System.arraycopy(columns, pos, tmpColumns, pos + 1, columns.length - pos);
    columns = tmpColumns;

    deviceIDs = null;
  }

  @Override
  public void swapColumn(int src, int target) {
    super.swapColumn(src, target);
    if (nullBitMaps != null) {
      CommonUtils.swapArray(nullBitMaps, src, target);
    }
    CommonUtils.swapArray(columns, src, target);
    deviceIDs = null;
  }

  public boolean isNull(int row, int col) {
    if (nullBitMaps == null || nullBitMaps[col] == null) {
      return false;
    }
    return nullBitMaps[col].isMarked(row);
  }
}
