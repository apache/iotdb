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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.DataTypeInconsistentException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.storageengine.dataregion.IObjectPath;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AbstractMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.db.utils.ObjectTypeUtils.generateObjectBinary;

public class RelationalInsertTabletNode extends InsertTabletNode {

  // deviceId cache for Table-view insertion
  private IDeviceID[] deviceIDs;

  private boolean singleDevice;

  public RelationalInsertTabletNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      MeasurementSchema[] measurementSchemas,
      long[] times,
      BitMap[] bitMaps,
      Object[] columns,
      int rowCount,
      TsTableColumnCategory[] columnCategories) {
    super(
        id,
        devicePath,
        isAligned,
        measurements,
        dataTypes,
        measurementSchemas,
        times,
        bitMaps,
        columns,
        rowCount);
    setColumnCategories(columnCategories);
  }

  public RelationalInsertTabletNode(PlanNodeId id) {
    super(id);
  }

  @TestOnly
  public RelationalInsertTabletNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      long[] times,
      BitMap[] bitMaps,
      Object[] columns,
      int rowCount,
      TsTableColumnCategory[] columnCategories) {
    super(id, devicePath, isAligned, measurements, dataTypes, times, bitMaps, columns, rowCount);
    setColumnCategories(columnCategories);
  }

  public void setSingleDevice() {
    this.singleDevice = true;
  }

  public List<Binary[]> getObjectColumns() {
    List<Binary[]> objectColumns = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      if (dataTypes[i] == TSDataType.OBJECT) {
        objectColumns.add((Binary[]) columns[i]);
      }
    }
    return objectColumns;
  }

  @Override
  public IDeviceID getDeviceID(int rowIdx) {
    if (singleDevice) {
      if (deviceIDs == null) {
        deviceIDs = new IDeviceID[1];
      }
      if (deviceIDs[0] == null) {
        String[] deviceIdSegments = new String[tagColumnIndices.size() + 1];
        deviceIdSegments[0] = this.getTableName();
        for (int i = 0; i < tagColumnIndices.size(); i++) {
          final Integer columnIndex = tagColumnIndices.get(i);
          Object idSeg = ((Object[]) columns[columnIndex])[0];
          boolean isNull =
              bitMaps != null && bitMaps[columnIndex] != null && bitMaps[columnIndex].isMarked(0);
          deviceIdSegments[i + 1] = !isNull && idSeg != null ? idSeg.toString() : null;
        }
        deviceIDs[0] = Factory.DEFAULT_FACTORY.create(deviceIdSegments);
      }
      return deviceIDs[0];
    }
    if (deviceIDs == null) {
      deviceIDs = new IDeviceID[rowCount];
    }
    if (deviceIDs[rowIdx] == null) {
      String[] deviceIdSegments = new String[tagColumnIndices.size() + 1];
      deviceIdSegments[0] = this.getTableName();
      for (int i = 0; i < tagColumnIndices.size(); i++) {
        final Integer columnIndex = tagColumnIndices.get(i);
        Object idSeg = ((Object[]) columns[columnIndex])[rowIdx];
        boolean isNull =
            bitMaps != null
                && bitMaps[columnIndex] != null
                && bitMaps[columnIndex].isMarked(rowIdx);
        deviceIdSegments[i + 1] = !isNull && idSeg != null ? idSeg.toString() : null;
      }
      IDeviceID currentDeviceId = Factory.DEFAULT_FACTORY.create(deviceIdSegments);
      if (rowIdx > 0 && currentDeviceId.equals(deviceIDs[rowIdx - 1])) {
        deviceIDs[rowIdx] = deviceIDs[rowIdx - 1];
      } else {
        deviceIDs[rowIdx] = currentDeviceId;
      }
    }

    return deviceIDs[rowIdx];
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitRelationalInsertTablet(this, context);
  }

  @Override
  protected InsertTabletNode getEmptySplit(int count) {
    long[] subTimes = new long[count];
    Object[] values = initTabletValues(dataTypes.length, count, dataTypes);
    BitMap[] newBitMaps = this.bitMaps == null ? null : initBitmaps(dataTypes.length, count);
    RelationalInsertTabletNode split =
        new RelationalInsertTabletNode(
            getPlanNodeId(),
            targetPath,
            isAligned,
            measurements,
            dataTypes,
            measurementSchemas,
            subTimes,
            newBitMaps,
            values,
            subTimes.length,
            columnCategories);
    if (singleDevice) {
      split.setSingleDevice();
    }
    return split;
  }

  protected Map<TRegionReplicaSet, List<Integer>> splitByReplicaSet(
      final Map<IDeviceID, PartitionSplitInfo> deviceIDSplitInfoMap, final IAnalysis analysis) {
    final Map<TRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();
    final Map<IDeviceID, TEndPoint> endPointMap = new HashMap<>();

    for (final Map.Entry<IDeviceID, PartitionSplitInfo> entry : deviceIDSplitInfoMap.entrySet()) {
      final IDeviceID deviceID = entry.getKey();
      final PartitionSplitInfo splitInfo = entry.getValue();
      final List<TRegionReplicaSet> replicaSets =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetForWriting(
                  deviceID, splitInfo.timePartitionSlots, analysis.getDatabaseName());
      splitInfo.replicaSets = replicaSets;
      // collect redirectInfo
      endPointMap.put(
          deviceID,
          replicaSets
              .get(replicaSets.size() - 1)
              .getDataNodeLocations()
              .get(0)
              .getClientRpcEndPoint());
      for (int i = 0; i < replicaSets.size(); i++) {
        final List<Integer> subRanges =
            splitMap.computeIfAbsent(replicaSets.get(i), x -> new ArrayList<>());
        subRanges.add(splitInfo.ranges.get(2 * i));
        subRanges.add(splitInfo.ranges.get(2 * i + 1));
      }
    }
    final List<TEndPoint> redirectNodeList = new ArrayList<>(times.length);
    for (int i = 0; i < times.length; i++) {
      final IDeviceID deviceId = getDeviceID(i);
      redirectNodeList.add(endPointMap.get(deviceId));
    }
    analysis.setRedirectNodeList(redirectNodeList);
    return splitMap;
  }

  public static RelationalInsertTabletNode deserialize(ByteBuffer byteBuffer) {
    RelationalInsertTabletNode insertNode = new RelationalInsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserialize(byteBuffer);
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    super.serializeAttributes(byteBuffer);
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        columnCategories[i].serialize(byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    super.serializeAttributes(stream);
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        columnCategories[i].serialize(stream);
      }
    }
  }

  @Override
  public void subDeserialize(ByteBuffer buffer) {
    super.subDeserialize(buffer);
    TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      columnCategories[i] = TsTableColumnCategory.deserialize(buffer);
    }
    setColumnCategories(columnCategories);
  }

  @Override
  void subSerialize(IWALByteBufferView buffer, List<int[]> rangeList) {
    super.subSerialize(buffer, rangeList);
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        buffer.put(columnCategories[i].getCategory());
      }
    }
  }

  @Override
  protected void subDeserializeFromWAL(ByteBuffer buffer) {
    super.subDeserializeFromWAL(buffer);
    TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      columnCategories[i] = TsTableColumnCategory.deserialize(buffer);
    }
    setColumnCategories(columnCategories);
  }

  @Override
  protected void subDeserializeFromWAL(DataInputStream stream) throws IOException {
    super.subDeserializeFromWAL(stream);
    TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      columnCategories[i] = TsTableColumnCategory.deserialize(stream);
    }
    setColumnCategories(columnCategories);
  }

  /** Deserialize from wal */
  public static RelationalInsertTabletNode deserializeFromWAL(DataInputStream stream)
      throws IOException {
    // we do not store plan node id in wal entry
    RelationalInsertTabletNode insertNode = new RelationalInsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserializeFromWAL(stream);
    return insertNode;
  }

  public static RelationalInsertTabletNode deserializeFromWAL(ByteBuffer buffer) {
    // we do not store plan node id in wal entry
    RelationalInsertTabletNode insertNode = new RelationalInsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserializeFromWAL(buffer);
    return insertNode;
  }

  @Override
  int subSerializeSize(int start, int end) {
    return super.subSerializeSize(start, end) + columnCategories.length * Byte.BYTES;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.RELATIONAL_INSERT_TABLET;
  }

  @Override
  public List<Pair<IDeviceID, Integer>> splitByDevice(int start, int end) {
    List<Pair<IDeviceID, Integer>> result = new ArrayList<>();
    IDeviceID prevDeviceId = getDeviceID(start);

    int i = start + 1;
    for (; i < end; i++) {
      IDeviceID currentDeviceId = getDeviceID(i);
      if (!currentDeviceId.equals(prevDeviceId)) {
        result.add(new Pair<>(prevDeviceId, i));
        prevDeviceId = getDeviceID(i);
      }
    }
    result.add(new Pair<>(prevDeviceId, end));

    return result;
  }

  @Override
  public int checkTTL(TSStatus[] results, long ttl) throws OutOfTTLException {
    return checkTTLInternal(results, ttl, false);
  }

  @Override
  public String getTableName() {
    return targetPath.getFullPath();
  }

  @Override
  protected PartialPath readTargetPath(ByteBuffer buffer) {
    return new PartialPath(ReadWriteIOUtils.readString(buffer), false);
  }

  @Override
  protected PartialPath readTargetPath(DataInputStream stream) throws IOException {
    return new PartialPath(ReadWriteIOUtils.readString(stream), false);
  }

  @Override
  public void updateLastCache(final String databaseName) {
    final String[] rawMeasurements = getRawMeasurements();

    final List<Pair<IDeviceID, Integer>> deviceEndOffsetPairs = splitByDevice(0, rowCount);
    int startOffset = 0;
    for (final Pair<IDeviceID, Integer> deviceEndOffsetPair : deviceEndOffsetPairs) {
      final IDeviceID deviceID = deviceEndOffsetPair.getLeft();
      final int endOffset = deviceEndOffsetPair.getRight();

      final TimeValuePair[] timeValuePairs = new TimeValuePair[rawMeasurements.length];
      for (int i = 0; i < rawMeasurements.length; i++) {
        timeValuePairs[i] = composeLastTimeValuePair(i, startOffset, endOffset);
      }
      TableDeviceSchemaCache.getInstance()
          .updateLastCacheIfExists(databaseName, deviceID, rawMeasurements, timeValuePairs);

      startOffset = endOffset;
    }
  }

  @Override
  protected List<WritePlanNode> doSplit(Map<TRegionReplicaSet, List<Integer>> splitMap) {
    List<WritePlanNode> result = new ArrayList<>();

    if (splitMap.size() == 1) {
      final Entry<TRegionReplicaSet, List<Integer>> entry = splitMap.entrySet().iterator().next();
      if (entry.getValue().size() == 2) {
        // Avoid using system arraycopy when there is no need to split
        setRange(entry.getValue());
        setDataRegionReplicaSet(entry.getKey());
        for (int i = 0; i < columns.length; i++) {
          if (dataTypes[i] == TSDataType.OBJECT) {
            handleObjectValue(i, 0, times.length, entry, result);
          }
        }
        result.add(this);
        return result;
      }
    }

    for (Map.Entry<TRegionReplicaSet, List<Integer>> entry : splitMap.entrySet()) {
      result.addAll(generateOneSplitList(entry));
    }
    return result;
  }

  private List<WritePlanNode> generateOneSplitList(
      Map.Entry<TRegionReplicaSet, List<Integer>> entry) {
    List<Integer> locs;
    // generate a new times and values
    locs = entry.getValue();
    int count = 0;
    for (int i = 0; i < locs.size(); i += 2) {
      int start = locs.get(i);
      int end = locs.get(i + 1);
      count += end - start;
    }

    List<WritePlanNode> result = new ArrayList<>();

    final InsertTabletNode subNode = getEmptySplit(count);
    int destLoc = 0;

    for (int k = 0; k < locs.size(); k += 2) {
      int start = locs.get(k);
      int end = locs.get(k + 1);
      final int length = end - start;

      System.arraycopy(times, start, subNode.times, destLoc, length);
      for (int i = 0; i < subNode.columns.length; i++) {
        if (dataTypes[i] != null) {
          if (dataTypes[i] == TSDataType.OBJECT) {
            handleObjectValue(i, start, end, entry, result);
          }
          System.arraycopy(columns[i], start, subNode.columns[i], destLoc, length);
        }
        if (subNode.bitMaps != null && this.bitMaps[i] != null) {
          BitMap.copyOfRange(this.bitMaps[i], start, subNode.bitMaps[i], destLoc, length);
        }
      }
      destLoc += length;
    }
    subNode.setFailedMeasurementNumber(getFailedMeasurementNumber());
    subNode.setRange(locs);
    subNode.setDataRegionReplicaSet(entry.getKey());
    result.add(subNode);
    return result;
  }

  private void handleObjectValue(
      int column,
      int startRow,
      int endRow,
      Map.Entry<TRegionReplicaSet, List<Integer>> entry,
      List<WritePlanNode> result) {
    for (int j = startRow; j < endRow; j++) {
      if (((Binary[]) columns[column])[j] == null) {
        continue;
      }
      byte[] binary = ((Binary[]) columns[column])[j].getValues();
      if (binary == null || binary.length == 0) {
        continue;
      }
      ByteBuffer buffer = ByteBuffer.wrap(binary);
      boolean isEoF = buffer.get() == 1;
      long offset = buffer.getLong();
      byte[] content = ReadWriteIOUtils.readBytes(buffer, buffer.remaining());
      IObjectPath relativePath =
          IObjectPath.Factory.FACTORY.create(
              entry.getKey().getRegionId().getId(), times[j], getDeviceID(j), measurements[column]);
      ObjectNode objectNode = new ObjectNode(isEoF, offset, content, relativePath);
      objectNode.setDataRegionReplicaSet(entry.getKey());
      result.add(objectNode);
      if (isEoF) {
        ((Binary[]) columns[column])[j] =
            generateObjectBinary(offset + content.length, relativePath);
      } else {
        ((Binary[]) columns[column])[j] = null;
        if (bitMaps == null) {
          bitMaps = new BitMap[columns.length];
        }
        if (bitMaps[column] == null) {
          bitMaps[column] = new BitMap(rowCount);
        }
        bitMaps[column].mark(j);
      }
    }
  }

  @Override
  public void checkDataType(AbstractMemTable memTable) throws DataTypeInconsistentException {
    if (singleDevice) {
      IWritableMemChunkGroup writableMemChunkGroup =
          memTable.getWritableMemChunkGroup(getDeviceID(0));
      if (writableMemChunkGroup != null) {
        writableMemChunkGroup.checkDataType(this);
      }
    } else {
      for (int i = 0; i < rowCount; i++) {
        IWritableMemChunkGroup writableMemChunkGroup =
            memTable.getWritableMemChunkGroup(getDeviceID(i));
        if (writableMemChunkGroup != null) {
          writableMemChunkGroup.checkDataType(this);
        }
      }
    }
  }
}
