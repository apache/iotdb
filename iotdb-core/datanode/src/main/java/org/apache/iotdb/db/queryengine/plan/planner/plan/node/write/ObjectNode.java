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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.calc.utils.ObjectTypeUtils.generateObjectBinary;

public class ObjectNode extends SearchNode implements WALEntryValue {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectNode.class);

  private final boolean isEOF;

  private final long offset;

  private byte[] content;

  private IObjectPath filePath;

  private final int contentLength;

  private TRegionReplicaSet dataRegionReplicaSet;

  private boolean isGeneratedByRemoteConsensusLeader;

  private ProgressIndex progressIndex;

  /**
   * Subclass-only constructor for delegation wrappers (e.g. pipe). Base field values are
   * placeholders; the subclass must override instance behavior to forward to the wrapped node.
   */
  protected ObjectNode(final PlanNodeId planNodeId) {
    super(planNodeId);
    this.isEOF = false;
    this.offset = 0L;
    this.filePath = null;
    this.content = null;
    this.contentLength = 0;
  }

  public ObjectNode(boolean isEOF, long offset, byte[] content, IObjectPath filePath) {
    super(new PlanNodeId(""));
    this.isEOF = isEOF;
    this.offset = offset;
    this.filePath = filePath;
    this.content = content;
    this.contentLength = content.length;
  }

  public ObjectNode(boolean isEOF, long offset, int contentLength, IObjectPath filePath) {
    super(new PlanNodeId(""));
    this.isEOF = isEOF;
    this.offset = offset;
    this.filePath = filePath;
    this.contentLength = contentLength;
  }

  public boolean isEOF() {
    return isEOF;
  }

  public byte[] getContent() {
    return content;
  }

  public long getOffset() {
    return offset;
  }

  public void setFilePath(IObjectPath filePath) {
    this.filePath = filePath;
  }

  public IObjectPath getFilePath() {
    return filePath;
  }

  public String getFilePathString() {
    return filePath.toString();
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    serializeToWAL(buffer, getEncodedSearchIndex());
  }

  public void serializeToWAL(IWALByteBufferView buffer, long encodedSearchIndex) {
    buffer.putShort(getType().getNodeType());
    buffer.putLong(encodedSearchIndex);
    buffer.put((byte) (isEOF ? 1 : 0));
    buffer.putLong(offset);
    try {
      filePath.serialize(buffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    buffer.putInt(content.length);
  }

  @Override
  public int serializedSize() {
    return Short.BYTES
        + Long.BYTES
        + Byte.BYTES
        + Long.BYTES
        + Integer.BYTES
        + filePath.getSerializedSize();
  }

  public static ObjectNode deserializeFromWAL(DataInputStream stream) throws IOException {
    long searchIndex = stream.readLong();
    boolean isEOF = stream.readByte() == 1;
    long offset = stream.readLong();
    IObjectPath filePath = IObjectPath.getDeserializer().deserializeFrom(stream);
    int contentLength = stream.readInt();
    ObjectNode objectNode = new ObjectNode(isEOF, offset, contentLength, filePath);
    objectNode.setSearchIndexFromWAL(searchIndex);
    return objectNode;
  }

  public static ObjectNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    boolean isEOF = buffer.get() == 1;
    long offset = buffer.getLong();
    IObjectPath filePath = IObjectPath.getDeserializer().deserializeFrom(buffer);
    Optional<File> objectFile =
        TierManager.getInstance().getAbsoluteObjectFilePath(filePath.toString());
    int contentLength = buffer.getInt();
    byte[] contents = new byte[contentLength];
    if (objectFile.isPresent()) {
      try (RandomAccessFile raf = new RandomAccessFile(objectFile.get(), "r")) {
        raf.seek(offset);
        raf.readFully(contents);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new ObjectFileNotExist(filePath.toString());
    }

    ObjectNode objectNode = new ObjectNode(isEOF, offset, contents, filePath);
    objectNode.setSearchIndexFromWAL(searchIndex);
    return objectNode;
  }

  public static ObjectNode deserialize(ByteBuffer byteBuffer) {
    boolean isEoF = ReadWriteIOUtils.readBool(byteBuffer);
    long offset = ReadWriteIOUtils.readLong(byteBuffer);
    IObjectPath filePath = IObjectPath.getDeserializer().deserializeFrom(byteBuffer);
    int contentLength = ReadWriteIOUtils.readInt(byteBuffer);
    byte[] content = ReadWriteIOUtils.readBytes(byteBuffer, contentLength);
    return new ObjectNode(isEoF, offset, content, filePath);
  }

  @Override
  public SearchNode merge(List<SearchNode> searchNodes) {
    if (searchNodes.size() == 1) {
      return searchNodes.get(0);
    }
    throw new UnsupportedOperationException(DataNodeQueryMessages.MERGE_IS_NOT_SUPPORTED);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    return null;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return dataRegionReplicaSet;
  }

  public void setDataRegionReplicaSet(TRegionReplicaSet dataRegionReplicaSet) {
    this.dataRegionReplicaSet = dataRegionReplicaSet;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(isEOF, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
    filePath.serialize(byteBuffer);
    ReadWriteIOUtils.write(contentLength, byteBuffer);
    byteBuffer.put(content);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(isEOF, stream);
    ReadWriteIOUtils.write(offset, stream);
    filePath.serialize(stream);
    ReadWriteIOUtils.write(contentLength, stream);
    stream.write(content);
  }

  public ByteBuffer serialize() {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream stream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(WALEntryType.OBJECT_FILE_NODE.getCode(), stream);
      ReadWriteIOUtils.write((long) TsFileProcessor.MEMTABLE_NOT_EXIST, stream);
      ReadWriteIOUtils.write(getType().getNodeType(), stream);
      byte[] contents = new byte[contentLength];
      boolean readSuccess = false;
      IOException ioException = null;
      for (int i = 0; i < 2; i++) {
        Optional<File> objectFile =
            TierManager.getInstance().getAbsoluteObjectFilePath(filePath.toString());
        if (objectFile.isPresent()) {
          try {
            readContentFromFile(objectFile.get(), contents);
            readSuccess = true;
          } catch (IOException e) {
            ioException = e;
          }
          if (readSuccess) {
            break;
          }
        }
        Optional<File> objectTmpFile =
            TierManager.getInstance().getAbsoluteObjectFilePath(filePath + ".tmp");
        if (objectTmpFile.isPresent()) {
          try {
            readContentFromFile(objectTmpFile.get(), contents);
            readSuccess = true;
          } catch (IOException e) {
            ioException = e;
          }
          if (readSuccess) {
            break;
          }
        }
      }
      // TTL or delete may remove the object file, DO NOT throw Exception here
      if (!readSuccess && LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            DataNodeQueryMessages.ERROR_WHEN_READ_OBJECT_FILE, filePath.toString(), ioException);
      }
      ReadWriteIOUtils.write(readSuccess && isEOF, stream);
      ReadWriteIOUtils.write(offset, stream);
      filePath.serialize(stream);
      ReadWriteIOUtils.write(contentLength, stream);
      stream.write(contents);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      throw new SerializationRunTimeException(e);
    }
  }

  private void readContentFromFile(File file, byte[] contents) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(offset);
      raf.readFully(contents);
    }
  }

  public RelationalInsertRowNode genValueInsertRowNode(final TableSchema tableSchema)
      throws IllegalPathException {
    return genValueInsertRowNodeWithTableSchema(tableSchema);
  }

  public RelationalInsertRowNode genValueInsertRowNode() throws IllegalPathException {
    return buildInsertRowNode(
        filePath.getDeviceID(),
        new String[] {filePath.getMeasurement()},
        new TSDataType[] {TSDataType.OBJECT},
        new MeasurementSchema[] {
          new MeasurementSchema(filePath.getMeasurement(), TSDataType.OBJECT)
        },
        new TsTableColumnCategory[] {TsTableColumnCategory.FIELD},
        new Object[] {generateObjectBinary(offset + contentLength, filePath)});
  }

  private RelationalInsertRowNode genValueInsertRowNodeWithTableSchema(
      final TableSchema tableSchema) throws IllegalPathException {
    final IDeviceID deviceID = filePath.getDeviceID();
    final List<IMeasurementSchema> allColumns = tableSchema.getColumnSchemas();
    final List<ColumnCategory> categories = tableSchema.getColumnTypes();
    final int validColCount = countValidColumns(allColumns, categories);
    final String[] measurements = new String[validColCount];
    final TSDataType[] dataTypes = new TSDataType[validColCount];
    final MeasurementSchema[] measurementSchemas = new MeasurementSchema[validColCount];
    final TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[validColCount];
    final Object[] values = new Object[validColCount];
    int idx = 0;
    int tagOrdinal = 0;
    for (int i = 0; i < allColumns.size(); i++) {
      final IMeasurementSchema col = allColumns.get(i);
      final ColumnCategory category = categories.get(i);
      if (!includeInObjectValueRow(category, col.getMeasurementName())) {
        if (category == ColumnCategory.TAG) {
          tagOrdinal++;
        }
        continue;
      }
      final String colName = col.getMeasurementName();
      final TSDataType dataType = col.getType();
      measurements[idx] = colName;
      dataTypes[idx] = dataType;
      measurementSchemas[idx] = new MeasurementSchema(colName, dataType);
      columnCategories[idx] = TsTableColumnCategory.fromTsFileColumnCategory(category);
      values[idx] = extractColumnValue(category, colName, tagOrdinal, deviceID);
      if (category == ColumnCategory.TAG) {
        tagOrdinal++;
      }
      idx++;
    }
    return buildInsertRowNode(
        deviceID, measurements, dataTypes, measurementSchemas, columnCategories, values);
  }

  private int countValidColumns(
      final List<IMeasurementSchema> columns, final List<ColumnCategory> categories) {
    int count = 0;
    for (int i = 0; i < columns.size(); i++) {
      if (includeInObjectValueRow(categories.get(i), columns.get(i).getMeasurementName())) {
        count++;
      }
    }
    return count;
  }

  private boolean includeInObjectValueRow(final ColumnCategory category, final String columnName) {
    if (category == ColumnCategory.TIME || category == ColumnCategory.ATTRIBUTE) {
      return false;
    }
    return category == ColumnCategory.TAG || columnName.equals(filePath.getMeasurement());
  }

  private Object extractColumnValue(
      final ColumnCategory category,
      final String columnName,
      final int tagOrdinal,
      final IDeviceID deviceID) {
    switch (category) {
      case TAG:
        return extractTagValue(tagOrdinal, deviceID);
      case FIELD:
        return extractFieldValue(columnName);
      default:
        return null;
    }
  }

  private Binary extractTagValue(final int tagOrdinal, final IDeviceID deviceID) {
    if (deviceID == null) {
      return Binary.EMPTY_VALUE;
    }
    if (tagOrdinal >= 0 && tagOrdinal + 1 < deviceID.segmentNum()) {
      final Object segment = deviceID.segment(tagOrdinal + 1);
      if (segment != null) {
        return new Binary(segment.toString().getBytes(StandardCharsets.UTF_8));
      }
    }
    return Binary.EMPTY_VALUE;
  }

  private Object extractFieldValue(final String columnName) {
    if (columnName.equals(filePath.getMeasurement())) {
      return generateObjectBinary(offset + contentLength, filePath);
    }
    return null;
  }

  private RelationalInsertRowNode buildInsertRowNode(
      final IDeviceID deviceID,
      final String[] measurements,
      final TSDataType[] dataTypes,
      final MeasurementSchema[] measurementSchemas,
      final TsTableColumnCategory[] columnCategories,
      final Object[] values)
      throws IllegalPathException {
    final RelationalInsertRowNode insertRowNode = new RelationalInsertRowNode(this.getPlanNodeId());
    insertRowNode.setAligned(true);
    insertRowNode.setDeviceID(deviceID);
    insertRowNode.setTargetPath(new PartialPath(deviceID.getTableName(), false));
    insertRowNode.setTime(filePath.getTime());
    insertRowNode.setMeasurements(measurements);
    insertRowNode.setDataTypes(dataTypes);
    insertRowNode.setMeasurementSchemas(measurementSchemas);
    insertRowNode.setColumnCategories(columnCategories);
    insertRowNode.setValues(values);
    if (isGeneratedByPipe()) {
      insertRowNode.markAsGeneratedByPipe();
    }
    return insertRowNode;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.OBJECT_FILE_NODE;
  }

  @Override
  public long getMemorySize() {
    return contentLength;
  }

  @Override
  public void markAsGeneratedByRemoteConsensusLeader() {
    isGeneratedByRemoteConsensusLeader = true;
  }

  public boolean isGeneratedByRemoteConsensusLeader() {
    return isGeneratedByRemoteConsensusLeader;
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitWriteObjectFile(this, context);
  }
}
