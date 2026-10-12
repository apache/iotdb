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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.DataTypeInconsistentException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AbstractMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.NumericRowValues;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A numeric table row with its own plan type and wire format. FIELDs retain raw long bits and TAGs
 * retain strings through consensus and WAL recovery.
 */
public class RelationalInsertNumericRowNode extends AbstractInsertRowNode {
  private long time;
  private NumericRowValues numericValues;

  public RelationalInsertNumericRowNode(PlanNodeId id) {
    super(id);
    isAligned = true;
  }

  public RelationalInsertNumericRowNode(
      PlanNodeId id,
      PartialPath table,
      String[] measurements,
      TSDataType[] dataTypes,
      MeasurementSchema[] schemas,
      long time,
      NumericRowValues values,
      TsTableColumnCategory[] categories) {
    super(id, table, true, measurements, dataTypes);
    this.measurementSchemas = schemas;
    this.time = time;
    this.numericValues = values;
    setColumnCategories(categories);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.RELATIONAL_INSERT_NUMERIC_ROW;
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitRelationalInsertNumericRow(this, context);
  }

  @Override
  public InsertNode mergeInsertNode(List<InsertNode> nodes) {
    // One numeric row has exactly one device and timestamp, so it cannot be split into fragments.
    // InsertNode.merge returns the sole node before reaching this method.
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  @Override
  public long getMinTime() {
    return time;
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

  @Override
  public Object getValuesForMemoryAccounting() {
    return numericValues;
  }

  @Override
  public int getValueCount() {
    return numericValues == null ? 0 : numericValues.size();
  }

  @Override
  public boolean isNull(int index) {
    return index < 0 || index >= getValueCount() || numericValues.isNull(index);
  }

  @Override
  public Object getValue(int index) {
    return isNull(index) ? null : numericValues.getObject(index, getDataType(index));
  }

  public String getTableName() {
    return targetPath.getFullPath();
  }

  @Override
  public IDeviceID getDeviceID() {
    if (deviceID == null) {
      int tagCount = 0;
      for (int i = 0; columnCategories != null && i < columnCategories.length; i++) {
        if (columnCategories[i] == TsTableColumnCategory.TAG && isPresent(i)) {
          tagCount++;
        }
      }
      String[] segments = new String[tagCount + 1];
      segments[0] = getTableName();
      int tagIndex = 1;
      for (int i = 0; columnCategories != null && i < columnCategories.length; i++) {
        if (columnCategories[i] == TsTableColumnCategory.TAG && isPresent(i)) {
          segments[tagIndex++] = numericValues.getTag(i);
        }
      }
      deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(segments);
    }
    return deviceID;
  }

  private boolean isPresent(int index) {
    return measurements != null
        && index >= 0
        && index < measurements.length
        && measurements[index] != null
        && index < getValueCount()
        && columnCategories != null
        && index < columnCategories.length
        && columnCategories[index] != null
        && (measurementSchemas == null
            || index < measurementSchemas.length && measurementSchemas[index] != null);
  }

  @Override
  protected int getValidMeasurementNumber() {
    int count = 0;
    for (int i = 0; i < getValueCount(); i++) {
      if (isPresent(i)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public int getValidMeasurementNumber(boolean fieldOnly) {
    int count = 0;
    for (int i = 0; i < getValueCount(); i++) {
      if (isPresent(i) && isValidMeasurement(i, fieldOnly)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public void markFailedMeasurement(int index) {
    if (!isPresent(index)) {
      return;
    }
    measurements[index] = null;
    if (dataTypes != null && index < dataTypes.length) {
      dataTypes[index] = null;
    }
    numericValues.setNull(index);
    deviceID = null;
    measurementColumnCnt = -1;
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    TTimePartitionSlot slot = TimePartitionUtils.getTimePartitionSlot(time);
    dataRegionReplicaSet =
        analysis
            .getDataPartitionInfo()
            .getDataRegionReplicaSetForWriting(getDeviceID(), slot, analysis.getDatabaseName());
    analysis.setRedirectNodeList(
        Collections.singletonList(
            dataRegionReplicaSet.getDataNodeLocations().get(0).getClientRpcEndPoint()));
    return Collections.singletonList(this);
  }

  @Override
  public void addChild(PlanNode child) {
    /* A single-row write has no children. */
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public PlanNode clone() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLastCacheTimestamp() {
    return time;
  }

  @Override
  public boolean hasLastCacheValue(int index) {
    return isPresent(index) && !isNull(index) && isWritableFieldMeasurement(index);
  }

  @Override
  public TimeValuePair getLastCacheValue(int index) {
    return hasLastCacheValue(index)
        ? new TimeValuePair(time, TsPrimitiveType.getByType(getDataType(index), getValue(index)))
        : null;
  }

  @Override
  public void updateLastCache(String database) {
    TableDeviceSchemaCache.getInstance()
        .updateLastCacheIfExists(database, getDeviceID(), measurements, measurementSchemas, this);
  }

  @Override
  public void checkDataType(AbstractMemTable memTable) throws DataTypeInconsistentException {
    IWritableMemChunkGroup group = memTable.getWritableMemChunkGroup(getDeviceID());
    if (group != null) {
      group.checkDataType(this);
    }
  }

  private int bodySize() {
    // Fixed body fields: timestamp, table name, number of valid columns, and schema-present flag.
    int size =
        Long.BYTES + ReadWriteIOUtils.sizeToWrite(getTableName()) + Integer.BYTES + Byte.BYTES;
    for (int i = 0; i < getValueCount(); i++) {
      if (!isPresent(i)) {
        continue;
      }
      // Column identity: either the measurement name or the serialized measurement schema.
      size +=
          measurementSchemas == null
              ? ReadWriteIOUtils.sizeToWrite(measurements[i])
              : measurementSchemas[i].serializedSize();
      // Column data type, column category, and null marker.
      size += 3 * Byte.BYTES;
      if (!isNull(i)) {
        // Non-null TAGs carry a length-prefixed String; numeric FIELDs carry one long value.
        size +=
            columnCategories[i] == TsTableColumnCategory.TAG
                ? ReadWriteIOUtils.sizeToWrite(numericValues.getTag(i))
                : Long.BYTES;
      }
    }
    return size;
  }

  @Override
  protected int serializedAttributesSize() {
    return PlanNodeType.BYTES + bodySize();
  }

  @Override
  public int serializedSize() {
    return PlanNodeType.BYTES + Long.BYTES + bodySize();
  }

  @Override
  protected void serializeAttributes(ByteBuffer buffer) {
    getType().serialize(buffer);
    serializeBody(buffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    serializeBody(stream);
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    serializeToWAL(buffer, getEncodedSearchIndex());
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer, long encodedSearchIndex) {
    buffer.putShort(getType().getNodeType());
    buffer.putLong(encodedSearchIndex);
    serializeBody(buffer);
  }

  private void serializeBody(ByteBuffer buffer) {
    ReadWriteIOUtils.write(time, buffer);
    ReadWriteIOUtils.write(getTableName(), buffer);
    ReadWriteIOUtils.write(getValidMeasurementNumber(), buffer);
    ReadWriteIOUtils.write((byte) (measurementSchemas == null ? 0 : 1), buffer);
    for (int i = 0; i < getValueCount(); i++) {
      if (!isPresent(i)) {
        continue;
      }
      if (measurementSchemas == null) {
        ReadWriteIOUtils.write(measurements[i], buffer);
      } else {
        measurementSchemas[i].serializeTo(buffer);
      }
      ReadWriteIOUtils.write(
          getDataType(i) == null ? (byte) -1 : getDataType(i).serialize(), buffer);
      ReadWriteIOUtils.write(columnCategories[i].getCategory(), buffer);
      ReadWriteIOUtils.write((byte) (isNull(i) ? 1 : 0), buffer);
      if (!isNull(i)) {
        if (columnCategories[i] == TsTableColumnCategory.TAG) {
          ReadWriteIOUtils.write(numericValues.getTag(i), buffer);
        } else {
          ReadWriteIOUtils.write(numericValues.get(i), buffer);
        }
      }
    }
  }

  private void serializeBody(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(time, stream);
    ReadWriteIOUtils.write(getTableName(), stream);
    ReadWriteIOUtils.write(getValidMeasurementNumber(), stream);
    ReadWriteIOUtils.write((byte) (measurementSchemas == null ? 0 : 1), stream);
    for (int i = 0; i < getValueCount(); i++) {
      if (!isPresent(i)) {
        continue;
      }
      if (measurementSchemas == null) {
        ReadWriteIOUtils.write(measurements[i], stream);
      } else {
        measurementSchemas[i].serializeTo(stream);
      }
      ReadWriteIOUtils.write(
          getDataType(i) == null ? (byte) -1 : getDataType(i).serialize(), stream);
      ReadWriteIOUtils.write(columnCategories[i].getCategory(), stream);
      ReadWriteIOUtils.write((byte) (isNull(i) ? 1 : 0), stream);
      if (!isNull(i)) {
        if (columnCategories[i] == TsTableColumnCategory.TAG) {
          ReadWriteIOUtils.write(numericValues.getTag(i), stream);
        } else {
          ReadWriteIOUtils.write(numericValues.get(i), stream);
        }
      }
    }
  }

  private void serializeBody(IWALByteBufferView buffer) {
    WALWriteUtils.write(time, buffer);
    WALWriteUtils.write(getTableName(), buffer);
    WALWriteUtils.write(getValidMeasurementNumber(), buffer);
    WALWriteUtils.write((byte) (measurementSchemas == null ? 0 : 1), buffer);
    for (int i = 0; i < getValueCount(); i++) {
      if (!isPresent(i)) {
        continue;
      }
      if (measurementSchemas == null) {
        WALWriteUtils.write(measurements[i], buffer);
      } else {
        WALWriteUtils.write(measurementSchemas[i], buffer);
      }
      WALWriteUtils.write(getDataType(i) == null ? (byte) -1 : getDataType(i).serialize(), buffer);
      WALWriteUtils.write(columnCategories[i].getCategory(), buffer);
      WALWriteUtils.write((byte) (isNull(i) ? 1 : 0), buffer);
      if (!isNull(i)) {
        if (columnCategories[i] == TsTableColumnCategory.TAG) {
          WALWriteUtils.write(numericValues.getTag(i), buffer);
        } else {
          WALWriteUtils.write(numericValues.get(i), buffer);
        }
      }
    }
  }

  private static RelationalInsertNumericRowNode deserializeBody(ByteBuffer buffer) {
    RelationalInsertNumericRowNode node = new RelationalInsertNumericRowNode(new PlanNodeId(""));
    node.time = buffer.getLong();
    node.targetPath = new PartialPath(ReadWriteIOUtils.readString(buffer), false);
    int count = buffer.getInt();
    boolean hasSchemas = buffer.get() != 0;
    node.measurements = new String[count];
    node.dataTypes = new TSDataType[count];
    node.measurementSchemas = hasSchemas ? new MeasurementSchema[count] : null;
    TsTableColumnCategory[] categories = new TsTableColumnCategory[count];
    String[] tagPayload = new String[count];
    long[] valuePayload = new long[count];
    boolean[] nonNull = new boolean[count];
    for (int i = 0; i < count; i++) {
      if (hasSchemas) {
        node.measurementSchemas[i] = MeasurementSchema.deserializeFrom(buffer);
        node.measurements[i] = node.measurementSchemas[i].getMeasurementName();
      } else {
        node.measurements[i] = ReadWriteIOUtils.readString(buffer);
      }
      byte type = buffer.get();
      node.dataTypes[i] = type == -1 ? null : TSDataType.deserialize(type);
      categories[i] = TsTableColumnCategory.deserialize(buffer);
      nonNull[i] = buffer.get() == 0;
      if (nonNull[i]) {
        if (categories[i] == TsTableColumnCategory.TAG && node.dataTypes[i] == TSDataType.STRING) {
          tagPayload[i] = ReadWriteIOUtils.readString(buffer);
        } else {
          if (!NumericRowValues.supports(node.dataTypes[i])
              || categories[i] != TsTableColumnCategory.FIELD) {
            throw new UnSupportedDataTypeException(
                DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE + node.dataTypes[i]);
          }
          valuePayload[i] = buffer.getLong();
        }
      }
    }
    node.numericValues = buildNumericValues(categories, tagPayload, valuePayload, nonNull);
    node.setColumnCategories(categories);
    return node;
  }

  public static RelationalInsertNumericRowNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    RelationalInsertNumericRowNode node = deserializeBody(buffer);
    node.setSearchIndexFromWAL(searchIndex);
    return node;
  }

  private static RelationalInsertNumericRowNode deserializeBody(DataInputStream stream)
      throws IOException {
    RelationalInsertNumericRowNode node = new RelationalInsertNumericRowNode(new PlanNodeId(""));
    node.time = stream.readLong();
    node.targetPath = new PartialPath(ReadWriteIOUtils.readString(stream), false);
    int count = stream.readInt();
    boolean hasSchemas = stream.readByte() != 0;
    node.measurements = new String[count];
    node.dataTypes = new TSDataType[count];
    node.measurementSchemas = hasSchemas ? new MeasurementSchema[count] : null;
    TsTableColumnCategory[] categories = new TsTableColumnCategory[count];
    String[] tagPayload = new String[count];
    long[] valuePayload = new long[count];
    boolean[] nonNull = new boolean[count];
    for (int i = 0; i < count; i++) {
      if (hasSchemas) {
        node.measurementSchemas[i] = MeasurementSchema.deserializeFrom(stream);
        node.measurements[i] = node.measurementSchemas[i].getMeasurementName();
      } else {
        node.measurements[i] = ReadWriteIOUtils.readString(stream);
      }
      byte type = stream.readByte();
      node.dataTypes[i] = type == -1 ? null : TSDataType.deserialize(type);
      categories[i] = TsTableColumnCategory.deserialize(stream);
      nonNull[i] = stream.readByte() == 0;
      if (nonNull[i]) {
        if (categories[i] == TsTableColumnCategory.TAG && node.dataTypes[i] == TSDataType.STRING) {
          tagPayload[i] = ReadWriteIOUtils.readString(stream);
        } else {
          if (!NumericRowValues.supports(node.dataTypes[i])
              || categories[i] != TsTableColumnCategory.FIELD) {
            throw new UnSupportedDataTypeException(
                DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE + node.dataTypes[i]);
          }
          valuePayload[i] = stream.readLong();
        }
      }
    }
    node.numericValues = buildNumericValues(categories, tagPayload, valuePayload, nonNull);
    node.setColumnCategories(categories);
    return node;
  }

  private static NumericRowValues buildNumericValues(
      TsTableColumnCategory[] categories,
      String[] tagPayload,
      long[] valuePayload,
      boolean[] nonNull) {
    int tagCount = 0;
    while (tagCount < categories.length && categories[tagCount] == TsTableColumnCategory.TAG) {
      tagCount++;
    }
    for (int i = tagCount; i < categories.length; i++) {
      if (categories[i] == TsTableColumnCategory.TAG) {
        throw new IllegalArgumentException();
      }
    }
    NumericRowValues values = new NumericRowValues(tagCount, categories.length - tagCount);
    for (int i = 0; i < categories.length; i++) {
      if (!nonNull[i]) {
        values.setNull(i);
      } else if (i < tagCount) {
        values.setTag(i, tagPayload[i]);
      } else {
        values.setRawValue(i, valuePayload[i]);
      }
    }
    return values;
  }

  public static RelationalInsertNumericRowNode deserializeFromWAL(DataInputStream stream)
      throws IOException {
    long searchIndex = stream.readLong();
    RelationalInsertNumericRowNode node = deserializeBody(stream);
    node.setSearchIndexFromWAL(searchIndex);
    return node;
  }

  public static RelationalInsertNumericRowNode deserialize(ByteBuffer buffer) {
    RelationalInsertNumericRowNode node = deserializeBody(buffer);
    node.setPlanNodeId(PlanNodeId.deserialize(buffer));
    return node;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof RelationalInsertNumericRowNode node
        && super.equals(other)
        && time == node.time
        && Objects.equals(numericValues, node.numericValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), time, numericValues);
  }

  @Override
  public String toString() {
    return "RelationalInsertNumericRowNode{table="
        + targetPath
        + ", time="
        + time
        + ", values="
        + Arrays.toString(getValues())
        + '}';
  }
}
