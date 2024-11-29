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
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.TimeValuePair;
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

  @Override
  public IDeviceID getDeviceID(int rowIdx) {
    if (singleDevice) {
      if (deviceIDs == null) {
        deviceIDs = new IDeviceID[1];
      }
      if (deviceIDs[0] == null) {
        String[] deviceIdSegments = new String[idColumnIndices.size() + 1];
        deviceIdSegments[0] = this.getTableName();
        for (int i = 0; i < idColumnIndices.size(); i++) {
          final Integer columnIndex = idColumnIndices.get(i);
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
      String[] deviceIdSegments = new String[idColumnIndices.size() + 1];
      deviceIdSegments[0] = this.getTableName();
      for (int i = 0; i < idColumnIndices.size(); i++) {
        final Integer columnIndex = idColumnIndices.get(i);
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
      Map<IDeviceID, PartitionSplitInfo> deviceIDSplitInfoMap, IAnalysis analysis) {
    Map<TRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();
    Map<IDeviceID, TEndPoint> endPointMap = new HashMap<>();

    for (Map.Entry<IDeviceID, PartitionSplitInfo> entry : deviceIDSplitInfoMap.entrySet()) {
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
        List<Integer> subRanges =
            splitMap.computeIfAbsent(replicaSets.get(i), x -> new ArrayList<>());
        subRanges.add(splitInfo.ranges.get(2 * i));
        subRanges.add(splitInfo.ranges.get(2 * i + 1));
      }
    }
    List<TEndPoint> redirectNodeList = new ArrayList<>(times.length);
    for (int i = 0; i < times.length; i++) {
      IDeviceID deviceId = getDeviceID(i);
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
  public void updateLastCache(String databaseName) {
    String[] rawMeasurements = getRawMeasurements();

    List<Pair<IDeviceID, Integer>> deviceEndOffsetPairs = splitByDevice(0, rowCount);
    int startOffset = 0;
    for (Pair<IDeviceID, Integer> deviceEndOffsetPair : deviceEndOffsetPairs) {
      IDeviceID deviceID = deviceEndOffsetPair.getLeft();
      int endOffset = deviceEndOffsetPair.getRight();

      TimeValuePair[] timeValuePairs = new TimeValuePair[rawMeasurements.length];
      for (int i = 0; i < rawMeasurements.length; i++) {
        timeValuePairs[i] = composeLastTimeValuePair(i, startOffset, endOffset);
      }
      TableDeviceSchemaCache.getInstance()
          .updateLastCacheIfExists(databaseName, deviceID, rawMeasurements, timeValuePairs);

      startOffset = endOffset;
    }
  }
}
