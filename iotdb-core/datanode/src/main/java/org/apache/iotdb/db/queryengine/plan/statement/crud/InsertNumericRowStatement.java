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

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertNumericRow;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.NumericRowValues;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/** A table-only numeric row. Schema edits and partial failures keep the payload in long[]. */
public class InsertNumericRowStatement extends InsertBaseStatement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InsertNumericRowStatement.class);
  private long time;
  private NumericRowValues numericValues;
  private IDeviceID deviceID;
  // A candidate snapshot, checked again under the schema analysis lock before reuse.
  private Pair<Long, Long> preparedTableVersion;

  public InsertNumericRowStatement() {
    statementType = StatementType.INSERT_NUMERIC_ROW;
    writeToTable = true;
    isAligned = true;
  }

  public Pair<Long, Long> getPreparedTableVersion() {
    return preparedTableVersion;
  }

  public void setPreparedTableVersion(Pair<Long, Long> preparedTableVersion) {
    this.preparedTableVersion = preparedTableVersion;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public NumericRowValues getNumericValues() {
    return numericValues;
  }

  public long[] getValues() {
    return numericValues.getValues();
  }

  public void setNumericValues(NumericRowValues values) {
    numericValues = values;
    deviceID = null;
  }

  public int getValueCount() {
    return numericValues == null ? 0 : numericValues.size();
  }

  /** RPC cells carry typed primitive FIELD bits or a length-prefixed STRING TAG. */
  public void fillValues(ByteBuffer buffer) throws QueryProcessException {
    dataTypes = new TSDataType[measurements.length];
    for (int i = 0; i < dataTypes.length; i++) {
      byte type = buffer.get();
      if (type < 0 || type >= TSDataType.values().length) {
        throw new QueryProcessException(DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE + type);
      }
      dataTypes[i] = TSDataType.values()[type];
      if (columnCategories[i] == TsTableColumnCategory.TAG && dataTypes[i] != TSDataType.STRING) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages
                .QUERY_EXCEPTION_MISSING_OR_INVALID_COLUMN_CATEGORIES_FOR_TABLE_INSERTION_5DF990B9);
      }
      if (columnCategories[i] == TsTableColumnCategory.FIELD
          && !NumericRowValues.supports(dataTypes[i])) {
        if (dataTypes[i] == TSDataType.STRING) {
          throw new IllegalArgumentException(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_MISSING_OR_INVALID_COLUMN_CATEGORIES_FOR_TABLE_INSERTION_5DF990B9);
        }
        throw new UnSupportedDataTypeException(
            DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE + dataTypes[i]);
      }
    }
    int tagCount = countTagColumns(columnCategories);
    numericValues = new NumericRowValues(tagCount, measurements.length - tagCount);
    ColumnCategory[] categories = new ColumnCategory[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      categories[i] =
          columnCategories[i] == TsTableColumnCategory.TAG
              ? ColumnCategory.TAG
              : ColumnCategory.FIELD;
    }
    numericValues.setColumnCategories(categories);
    for (int i = 0; i < dataTypes.length; i++) {
      if (columnCategories[i] == TsTableColumnCategory.TAG) {
        numericValues.read(i, dataTypes[i], buffer);
      }
    }
    for (int i = 0; i < dataTypes.length; i++) {
      if (columnCategories[i] == TsTableColumnCategory.FIELD) {
        numericValues.read(i, dataTypes[i], buffer);
      }
    }
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertNumericRow(this, context);
  }

  @Override
  public Statement toRelationalStatement(MPPQueryContext context) {
    return new InsertNumericRow(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public long getMinTime() {
    return time;
  }

  public TTimePartitionSlot getTimePartitionSlot() {
    return TimePartitionUtils.getTimePartitionSlot(time);
  }

  @Override
  public boolean isEmpty() {
    return getValueCount() == 0;
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    return index < 0 || index >= getValueCount()
        ? null
        : numericValues.getObject(index, getDataType(index));
  }

  @Override
  public boolean isColumnPresent(int index) {
    return super.isColumnPresent(index) && index < getValueCount();
  }

  @Override
  protected boolean checkAndCastDataType(int index, TSDataType type) {
    if (index < 0
        || index >= getValueCount()
        || !numericValues.cast(index, getDataType(index), type)) {
      return false;
    }
    dataTypes[index] = type;
    return true;
  }

  @Override
  public void semanticCheck() {
    super.semanticCheck();
    if (measurements.length != getValueCount()) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages
                  .THE_MEASUREMENTLIST_S_SIZE_D_IS_NOT_CONSISTENT_WITH_THE_VALUELIST_S_SIZE_D,
              measurements.length,
              getValueCount()));
    }
  }

  @Override
  public void markFailedMeasurement(int index, Exception cause) {
    if (!isColumnPresent(index)) {
      return;
    }
    if (failedMeasurementIndex2Info == null) {
      failedMeasurementIndex2Info = new HashMap<>();
    }
    failedMeasurementIndex2Info.putIfAbsent(
        index,
        new FailedMeasurementInfo(
            measurements[index], getDataType(index), getFirstValueOfIndex(index), cause));
    measurements[index] = null;
    if (dataTypes != null && index < dataTypes.length) {
      dataTypes[index] = null;
    }
    numericValues.setNull(index);
    tagColumnIndices = null;
    deviceID = null;
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
          numericValues.setObject(index, info.getDataType(), info.getValue());
        });
    failedMeasurementIndex2Info.clear();
    tagColumnIndices = null;
    deviceID = null;
  }

  @Override
  public void insertColumn(int pos, ColumnSchema columnSchema) {
    super.insertColumn(pos, columnSchema);
    int[] mapping = new int[measurements.length];
    for (int i = 0; i < mapping.length; i++) {
      mapping[i] = i < pos ? i : i == pos ? -1 : i - 1;
    }
    numericValues = numericValues.remap(mapping, countTagColumns(columnCategories));
    deviceID = null;
  }

  @Override
  public void swapColumn(int src, int target) {
    super.swapColumn(src, target);
    int[] newToOld = new int[measurements.length];
    for (int i = 0; i < newToOld.length; i++) {
      newToOld[i] = i;
    }
    newToOld[src] = target;
    newToOld[target] = src;
    numericValues = numericValues.remap(newToOld, countTagColumns(columnCategories));
    deviceID = null;
  }

  @Override
  public void rebuildArraysAfterExpansion(int[] mapping, String[] newMeasurements) {
    super.rebuildArraysAfterExpansion(mapping, newMeasurements);
    numericValues = numericValues.remap(mapping, countTagColumns(columnCategories));
    deviceID = null;
  }

  @Override
  protected void subRemoveAttributeColumns(List<Integer> columnsToKeep) {
    numericValues =
        numericValues.remap(
            columnsToKeep.stream().mapToInt(Integer::intValue).toArray(),
            countTagColumns(columnCategories));
    deviceID = null;
  }

  @Override
  public void setColumnCategories(TsTableColumnCategory[] categories) {
    super.setColumnCategories(categories);
    if (numericValues != null) {
      ColumnCategory[] compactCategories = new ColumnCategory[categories.length];
      for (int i = 0; i < categories.length; i++) {
        compactCategories[i] =
            categories[i] == TsTableColumnCategory.TAG ? ColumnCategory.TAG : ColumnCategory.FIELD;
      }
      numericValues.setColumnCategories(compactCategories);
    }
    deviceID = null;
  }

  private static int countTagColumns(TsTableColumnCategory[] categories) {
    int count = 0;
    while (count < categories.length && categories[count] == TsTableColumnCategory.TAG) {
      count++;
    }
    return count;
  }

  public IDeviceID getTableDeviceID() {
    if (deviceID == null) {
      String[] segments = new String[getTagColumnIndices().size() + 1];
      segments[0] = getTableName();
      for (int i = 0; i < getTagColumnIndices().size(); i++) {
        segments[i + 1] = numericValues.getTag(getTagColumnIndices().get(i));
      }
      deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(segments);
    }
    return deviceID;
  }

  @Override
  protected long calculateBytesUsed() {
    return INSTANCE_SIZE
        + (numericValues == null ? 0 : numericValues.ramBytesUsed())
        + InsertNodeMemoryEstimator.sizeOfIDeviceID(deviceID);
  }

  // Tree schema validation and logical view splitting do not apply to numeric table rows.
  @Override
  public ISchemaValidation getSchemaValidation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ISchemaValidation> getSchemaValidationList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public InsertBaseStatement removeLogicalView() {
    return this;
  }

  @Override
  public String getPipeLoggingString() {
    return "InsertNumericRowStatement{table="
        + getTableName()
        + ", time="
        + time
        + ", valueLength="
        + getValueCount()
        + '}';
  }

  @Override
  public String toString() {
    return getPipeLoggingString() + Arrays.toString(getValues());
  }
}
