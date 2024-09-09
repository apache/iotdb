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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InsertNodeMemoryEstimator {

  private static final String INSERT_TABLET_NODE_CLASS_NAME =
      "org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode";
  private static final String INSERT_ROW_NODE_CLASS_NAME =
      "org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode";
  private static final String INSERT_ROWS_NODE_CLASS_NAME =
      "org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode";
  private static final String INSERT_ROWS_OF_ONE_DEVICE_NODE_CLASS_NAME =
      "org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode";
  private static final String INSERT_MULTI_TABLETS_NODE_CLASS_NAME =
      "org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode";
  private static final String RELATIONAL_INSERT_ROWS_NODE_CLASS_NAME =
      "org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode";
  private static final String RELATIONAL_INSERT_ROW_NODE_CLASS_NAME =
      "org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode";
  private static final String RELATIONAL_INSERT_TABLET_NODE_CLASS_NAME =
      "org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode";

  private static final long NUM_BYTES_OBJECT_REF = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  private static final long NUM_BYTES_OBJECT_HEADER = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
  private static final long NUM_BYTES_ARRAY_HEADER = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

  private static final long REENTRANT_READ_WRITE_LOCK_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ReentrantReadWriteLock.class);

  // =============================InsertNode==================================
  // Calculate the total size of primitive fields in InsertNode
  private static final long INSERT_NODE_PRIMITIVE_SIZE =
      2 * 2 // isAligned + isGeneratedByRemoteConsensusLeader
          + 2 * Integer.BYTES; // measurementColumnCnt +failedMeasurementNumber

  // Calculate the total size of reference types in InsertNode
  private static final long INSERT_NODE_REFERENCE_SIZE =
      9L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in InsertTabletNode
  private static final long INSERT_TABLET_NODE_PRIMITIVE_SIZE = Integer.BYTES;

  // Calculate the total size of reference types in InsertTabletNode
  private static final long INSERT_TABLET_NODE_REFERENCE_SIZE =
      4L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in InsertRowNode
  private static final long INSERT_ROW_NODE_PRIMITIVE_SIZE = Long.BYTES + 1;

  // Calculate the total size of reference types in InsertRowNode
  private static final long INSERT_ROW_NODE_REFERENCE_SIZE = RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in InsertRowsNode
  private static final long INSERT_ROWS_NODE_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in InsertRowsOfOneDeviceNode
  private static final long INSERT_ROWS_OF_ONE_DEVICE_NODE_REFERENCE_SIZE =
      3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in RelationalInsertRowNode
  private static final long RELATIONAL_INSERT_ROW_NODE_PRIMITIVE_SIZE = Long.BYTES + 1;

  // Calculate the total size of reference types in RelationalInsertRowNode
  private static final long RELATIONAL_INSERT_ROW_NODE_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in RelationalInsertRowsNode
  private static final long RELATIONAL_INSERT_ROWS_NODE_REFERENCE_SIZE =
      INSERT_ROWS_NODE_REFERENCE_SIZE + NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in InsertTabletNode
  private static final long RELATIONAL_INSERT_TABLET_NODE_PRIMITIVE_SIZE = Integer.BYTES;

  // Calculate the total size of reference types in InsertTabletNode
  private static final long RELATIONAL_INSERT_TABLET_NODE_REFERENCE_SIZE =
      5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // ============================Device And Measurement===================================
  // Calculate the total size of reference types in PartialPath
  private static final long PARTIAL_PATH_REFERENCE_SIZE =
      4L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in Measurement
  private static final long MEASUREMENT_PRIMITIVE_SIZE = 3L; // type + encoding +  compressor

  // Calculate the total size of reference types in Measurement
  private static final long MEASUREMENT_REFERENCE_SIZE =
      3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in PlainDeviceID
  private static final long PLAIN_DEVICE_ID_REFERENCE_SIZE =
      3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // =============================Thrift==================================

  // Calculate the total size of reference types in TRegionReplicaSet
  private static final long T_REGION_REPLICA_SET_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in TDataNodeLocation
  private static final long T_DATA_NODE_LOCATION_REFERENCE_SIZE =
      5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in TSStatus
  private static final long TS_STATUS_REFERENCE_SIZE = 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in TSStatus
  private static final long TS_STATUS_PRIMITIVE_SIZE = Integer.BYTES + 1;

  // =============================ProgressIndex==================================

  // Calculate the total size of reference types in HybridProgressIndex
  private static final long HYBRID_PROGRESS_INDEX_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in IoTProgressIndex
  private static final long IO_T_PROGRESS_INDEX_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in MetaProgressIndex
  private static final long META_PROGRESS_INDEX_REFERENCE_SIZE =
      RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in MetaProgressIndex
  private static final long META_PROGRESS_INDEX_PRIMITIVE_SIZE = Long.BYTES;

  // Calculate the total size of reference types in RecoverProgressIndex
  private static final long RECOVER_PROGRESS_INDEX_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of reference types in SimpleProgressIndex
  private static final long SIMPLE_PROGRESS_INDEX_REFERENCE_SIZE =
      RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in SimpleProgressIndex
  private static final long SIMPLE_PROGRESS_INDEX_PRIMITIVE_SIZE = Integer.BYTES + Long.BYTES;

  // Calculate the total size of reference types in StateProgressIndex
  private static final long STATE_PROGRESS_INDEX_REFERENCE_SIZE =
      3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in StateProgressIndex
  private static final long STATE_PROGRESS_INDEX_PRIMITIVE_SIZE = Long.BYTES;

  // Calculate the total size of reference types in TimeWindowStateProgressIndex
  private static final long TIME_WINDOW_STATE_PROGRESS_INDEX_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // =============================BitMap==================================
  // Calculate the total size of reference types in BitMap
  private static final long BIT_MAP_REFERENCE_SIZE = RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in BitMap
  private static final long BIT_MAP_PRIMITIVE_SIZE = Integer.BYTES;

  public long sizeOf(InsertNode insertNode) {
    String className = insertNode.getClass().getName();
    switch (className) {
      case INSERT_TABLET_NODE_CLASS_NAME:
        return sizeOfInsertTabletNode((InsertTabletNode) insertNode);
      case INSERT_ROW_NODE_CLASS_NAME:
        return sizeOfInsertRowNode((InsertRowNode) insertNode);
      case INSERT_ROWS_NODE_CLASS_NAME:
        return sizeOfInsertRowsNode((InsertRowsNode) insertNode);
      case INSERT_ROWS_OF_ONE_DEVICE_NODE_CLASS_NAME:
        return sizeOfInsertRowsOfOneDeviceNode((InsertRowsOfOneDeviceNode) insertNode);
      case INSERT_MULTI_TABLETS_NODE_CLASS_NAME:
        return sizeOfInsertMultiTabletsNode((InsertMultiTabletsNode) insertNode);
      case RELATIONAL_INSERT_ROWS_NODE_CLASS_NAME:
        return sizeOfRelationalInsertRowsNode((RelationalInsertRowsNode) insertNode);
      case RELATIONAL_INSERT_ROW_NODE_CLASS_NAME:
        return sizeOfRelationalInsertRowNode((RelationalInsertRowNode) insertNode);
      case RELATIONAL_INSERT_TABLET_NODE_CLASS_NAME:
        return sizeOfRelationalInsertTabletNode((RelationalInsertTabletNode) insertNode);
      default:
        return 0L;
    }
  }

  // =============================InsertNode==================================

  private long sizeOfInsertNode(final InsertNode node) {
    long size = 0;
    // PartialPath
    size += sizeOfPartialPath(node.getDevicePath());
    // MeasurementSchemas
    size += sizeOfMeasurementSchemas(node.getMeasurementSchemas());
    // Measurement
    size += sizeOfMeasurement(node.getMeasurements());
    // dataTypes
    size +=
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + node.getDataTypes().length * NUM_BYTES_OBJECT_REF);
    // columnCategories
    size +=
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + node.getColumnCategories().length * NUM_BYTES_OBJECT_REF);
    // idColumnIndices
    size += sizeOfColumnIndices(node.getColumnCategories());
    // deviceID
    size += sizeOfIDeviceID(node.getDeviceID());
    // dataRegionReplicaSet
    size += sizeOfTRegionReplicaSet(node.getRegionReplicaSet());
    // progressIndex
    size += sizeOfProgressIndex(node.getProgressIndex());
    return size;
  }

  private long sizeOfInsertTabletNode(final InsertTabletNode node) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + INSERT_NODE_PRIMITIVE_SIZE
                + INSERT_NODE_REFERENCE_SIZE
                + INSERT_TABLET_NODE_PRIMITIVE_SIZE
                + INSERT_TABLET_NODE_REFERENCE_SIZE);

    size += sizeOfInsertNode(node);

    size += sizeOfTimes(node.getTimes());

    size += sizeOfBitMapArray(node.getBitMaps());

    size += sizeOfValues(node.getColumns(), node.getMeasurementSchemas(), node.getBitMaps());

    final List<Integer> range = node.getRange();
    if (range != null) {
      size += NUM_BYTES_OBJECT_HEADER + (long) Integer.BYTES * range.size();
    }
    return size;
  }

  private long sizeOfInsertRowNode(final InsertRowNode node) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + INSERT_NODE_PRIMITIVE_SIZE
                + INSERT_NODE_REFERENCE_SIZE
                + INSERT_ROW_NODE_PRIMITIVE_SIZE
                + INSERT_ROW_NODE_REFERENCE_SIZE);

    size += sizeOfValues(node.getValues(), node.getMeasurementSchemas());
    return size;
  }

  private long sizeOfInsertRowsNode(final InsertRowsNode node) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + INSERT_NODE_PRIMITIVE_SIZE
                + INSERT_NODE_REFERENCE_SIZE
                + INSERT_ROWS_NODE_REFERENCE_SIZE);

    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      size += NUM_BYTES_OBJECT_HEADER;
      size += sizeOfInsertRowNode(rows.get(0)) * rows.size();

      size += NUM_BYTES_OBJECT_HEADER;
      size += (long) indexList.size() * Integer.SIZE;
    }
    return size;
  }

  private long sizeOfInsertRowsOfOneDeviceNode(final InsertRowsOfOneDeviceNode node) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + INSERT_NODE_PRIMITIVE_SIZE
                + INSERT_NODE_REFERENCE_SIZE
                + INSERT_ROWS_OF_ONE_DEVICE_NODE_REFERENCE_SIZE);

    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      size += NUM_BYTES_OBJECT_HEADER;
      size += sizeOfInsertRowNode(rows.get(0)) * rows.size();

      size += NUM_BYTES_OBJECT_HEADER;
      size += (long) indexList.size() * Integer.SIZE;
    }
    // results
    size += NUM_BYTES_OBJECT_HEADER;
    for (Map.Entry<Integer, TSStatus> entry : node.getResults().entrySet()) {
      size += Integer.SIZE + sizeOfTSStatus(entry.getValue());
    }
    return size;
  }

  private long sizeOfInsertMultiTabletsNode(final InsertMultiTabletsNode node) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + INSERT_NODE_PRIMITIVE_SIZE
                + INSERT_NODE_REFERENCE_SIZE
                + INSERT_ROWS_OF_ONE_DEVICE_NODE_REFERENCE_SIZE);

    final List<InsertTabletNode> rows = node.getInsertTabletNodeList();
    final List<Integer> indexList = node.getParentInsertTabletNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      size += NUM_BYTES_OBJECT_HEADER;
      size += sizeOfInsertTabletNode(rows.get(0)) * rows.size();

      size += NUM_BYTES_OBJECT_HEADER;
      size += (long) indexList.size() * Integer.SIZE;
    }
    // results
    size += NUM_BYTES_OBJECT_HEADER;
    for (Map.Entry<Integer, TSStatus> entry : node.getResults().entrySet()) {
      size += Integer.SIZE + sizeOfTSStatus(entry.getValue());
    }
    return size;
  }

  private long sizeOfRelationalInsertRowsNode(final RelationalInsertRowsNode node) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + INSERT_NODE_PRIMITIVE_SIZE
                + INSERT_NODE_REFERENCE_SIZE
                + RELATIONAL_INSERT_ROWS_NODE_REFERENCE_SIZE);

    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      size += NUM_BYTES_OBJECT_HEADER;
      size += sizeOfInsertRowNode(rows.get(0)) * rows.size();

      size += NUM_BYTES_OBJECT_HEADER;
      size += (long) indexList.size() * Integer.SIZE;
    }
    // ignore deviceIDs
    return size;
  }

  private long sizeOfRelationalInsertRowNode(final RelationalInsertRowNode node) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + INSERT_NODE_PRIMITIVE_SIZE
                + INSERT_NODE_REFERENCE_SIZE
                + RELATIONAL_INSERT_ROW_NODE_PRIMITIVE_SIZE
                + RELATIONAL_INSERT_ROW_NODE_REFERENCE_SIZE);

    size += sizeOfValues(node.getValues(), node.getMeasurementSchemas());
    size += sizeOfIDeviceID(node.getDeviceID());
    return size;
  }

  private long sizeOfRelationalInsertTabletNode(final RelationalInsertTabletNode node) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + INSERT_NODE_PRIMITIVE_SIZE
                + INSERT_NODE_REFERENCE_SIZE
                + RELATIONAL_INSERT_TABLET_NODE_PRIMITIVE_SIZE
                + RELATIONAL_INSERT_TABLET_NODE_REFERENCE_SIZE);

    size += sizeOfInsertNode(node);

    size += sizeOfTimes(node.getTimes());

    size += sizeOfBitMapArray(node.getBitMaps());

    size += sizeOfValues(node.getColumns(), node.getMeasurementSchemas(), node.getBitMaps());

    final List<Integer> range = node.getRange();
    if (range != null) {
      size += NUM_BYTES_OBJECT_HEADER + (long) Integer.BYTES * range.size();
    }
    // ignore deviceIDs
    return size;
  }

  // ============================Device And Measurement===================================

  private long sizeOfPartialPath(final PartialPath partialPath) {
    if (partialPath == null) {
      return 0L;
    }
    long size =
        RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + PARTIAL_PATH_REFERENCE_SIZE);
    final String[] nodes = partialPath.getNodes();
    if (nodes != null) {
      // Since fullPath may be lazy loaded, lazy loading will not be triggered here, so it is
      // assumed that the memory size of fullPath is the same as that of nodes.
      size += sizeOfStringArray(nodes) * 2;
      size += TsFileConstant.PATH_SEPARATOR.length() * (nodes.length - 1) + NUM_BYTES_OBJECT_HEADER;
    }
    return size;
  }

  private long sizeOfMeasurementSchemas(final MeasurementSchema[] measurementSchemas) {
    if (measurementSchemas == null) {
      return 0L;
    }
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * measurementSchemas.length);
    for (MeasurementSchema measurementSchema : measurementSchemas) {
      size += sizeOfMeasurementSchema(measurementSchema);
    }
    return size;
  }

  private long sizeOfMeasurementSchema(final MeasurementSchema measurementSchema) {
    if (measurementSchema == null) {
      return 0L;
    }
    // Header + primitive + reference
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + MEASUREMENT_PRIMITIVE_SIZE + MEASUREMENT_REFERENCE_SIZE);
    // measurementId
    size += sizeOfString(measurementSchema.getMeasurementId());
    // props
    final Map<String, String> props = measurementSchema.getProps();
    if (props != null) {
      size += NUM_BYTES_OBJECT_HEADER;
      for (Map.Entry<String, String> entry : props.entrySet()) {
        size += sizeOfString(entry.getKey()) + sizeOfString(entry.getValue());
      }
    }
    return size;
  }

  private long sizeOfMeasurement(final String[] measurement) {
    if (measurement == null) {
      return 0L;
    }
    return sizeOfStringArray(measurement);
  }

  private long sizeOfColumnIndices(final TsTableColumnCategory[] columnCategories) {
    long size = 0;
    if (columnCategories != null) {
      size = NUM_BYTES_ARRAY_HEADER;
      for (TsTableColumnCategory columnCategory : columnCategories) {
        if (columnCategory.equals(TsTableColumnCategory.ID)) {
          size += Integer.BYTES;
        }
      }
    }
    return size;
  }

  private long sizeOfIDeviceID(final IDeviceID deviceID) {
    if (deviceID == null) {
      return 0L;
    }
    if (deviceID instanceof PlainDeviceID) {
      return sizeOfPlainDeviceID((PlainDeviceID) deviceID);
    }

    return sizeOfStringArrayDeviceID((StringArrayDeviceID) deviceID);
  }

  private long sizeOfIDeviceIDArray(final IDeviceID[] deviceID) {
    if (deviceID == null) {
      return 0L;
    }
    long size = NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * deviceID.length;
    for (IDeviceID id : deviceID) {
      size += sizeOfIDeviceID(id);
    }

    return size;
  }

  private long sizeOfPlainDeviceID(final PlainDeviceID deviceID) {
    long size =
        RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + PLAIN_DEVICE_ID_REFERENCE_SIZE);
    final String id = deviceID.toString();
    // Other fields are parsed by device. Since the parsing is lazy loading, in order not to trigger
    // lazy loading, other fields are estimated to be the same length as deviceID.
    if (id != null) {
      size += sizeOfString(id) * 2;
    }

    return size;
  }

  private long sizeOfStringArrayDeviceID(final StringArrayDeviceID deviceID) {
    // Memory alignment of basic types and reference types in structures
    long size =
        RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + PLAIN_DEVICE_ID_REFERENCE_SIZE);

    final String id = deviceID.toString();
    if (id != null) {
      size += sizeOfString(id);
    }

    return size;
  }

  // =============================Thrift==================================

  private long sizeOfTRegionReplicaSet(final TRegionReplicaSet tRegionReplicaSet) {
    if (tRegionReplicaSet == null) {
      return 0L;
    }
    // Memory alignment of basic types and reference types in structures
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + T_REGION_REPLICA_SET_REFERENCE_SIZE);

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    if (tRegionReplicaSet.isSetRegionId()) {
      size += sizeOfTConsensusGroupId();
    }

    if (tRegionReplicaSet.isSetDataNodeLocations()) {
      size += NUM_BYTES_OBJECT_HEADER;
      for (TDataNodeLocation tDataNodeLocation : tRegionReplicaSet.getDataNodeLocations()) {
        size += sizeOfTDataNodeLocation(tDataNodeLocation);
      }
    }

    return size;
  }

  private long sizeOfTConsensusGroupId() {
    // objectHeader + type + id
    return RamUsageEstimator.alignObjectSize(
        NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF + Integer.BYTES);
  }

  private long sizeOfTDataNodeLocation(final TDataNodeLocation tDataNodeLocation) {
    if (tDataNodeLocation == null) {
      return 0L;
    }
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + Integer.BYTES + T_DATA_NODE_LOCATION_REFERENCE_SIZE);

    size += sizeOfTEndPoint(tDataNodeLocation.getClientRpcEndPoint());
    size += sizeOfTEndPoint(tDataNodeLocation.getInternalEndPoint());
    size += sizeOfTEndPoint(tDataNodeLocation.getMPPDataExchangeEndPoint());
    size += sizeOfTEndPoint(tDataNodeLocation.getDataRegionConsensusEndPoint());
    size += sizeOfTEndPoint(tDataNodeLocation.getSchemaRegionConsensusEndPoint());

    return size;
  }

  private long sizeOfTEndPoint(final TEndPoint tEndPoint) {
    if (tEndPoint == null) {
      return 0L;
    }
    // objectHeader + ip + port
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF + Integer.BYTES);

    size += sizeOfString(tEndPoint.ip);

    return size;
  }

  private long sizeOfTSStatus(final TSStatus tSStatus) {
    if (tSStatus == null) {
      return 0L;
    }
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + TS_STATUS_REFERENCE_SIZE + TS_STATUS_PRIMITIVE_SIZE);
    // message
    if (tSStatus.isSetMessage()) {
      size += sizeOfString(tSStatus.message);
    }

    // ignore subStatus
    // redirectNode
    if (tSStatus.isSetRedirectNode()) {
      size += sizeOfTEndPoint(tSStatus.redirectNode);
    }
    return size;
  }

  // =============================ProgressIndex==================================

  private long sizeOfProgressIndex(final ProgressIndex progressIndex) {
    if (progressIndex == null) {
      return 0L;
    }
    switch (progressIndex.getType()) {
      case HYBRID_PROGRESS_INDEX:
        return sizeOfHybridProgressIndex((HybridProgressIndex) progressIndex);
      case IOT_PROGRESS_INDEX:
        return sizeOfIoTProgressIndex((IoTProgressIndex) progressIndex);
      case META_PROGRESS_INDEX:
        return sizeOfMetaProgressIndex();
      case STATE_PROGRESS_INDEX:
        return sizeOfStateProgressIndex((StateProgressIndex) progressIndex);
      case SIMPLE_PROGRESS_INDEX:
        return sizeOfSimpleProgressIndex();
      case MINIMUM_PROGRESS_INDEX:
        return sizeOfMinimumProgressIndex();
      case RECOVER_PROGRESS_INDEX:
        return sizeOfRecoverProgressIndex((RecoverProgressIndex) progressIndex);
      case TIME_WINDOW_STATE_PROGRESS_INDEX:
        return sizeOfTimeWindowStateProgressIndex((TimeWindowStateProgressIndex) progressIndex);
    }
    return 0L;
  }

  private long sizeOfHybridProgressIndex(final HybridProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + HYBRID_PROGRESS_INDEX_REFERENCE_SIZE);

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    size +=
        NUM_BYTES_OBJECT_HEADER
            + progressIndex.getType2Index().size() * (Short.BYTES + NUM_BYTES_OBJECT_REF);
    return size;
  }

  private long sizeOfIoTProgressIndex(IoTProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + IO_T_PROGRESS_INDEX_REFERENCE_SIZE);

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    size +=
        NUM_BYTES_OBJECT_HEADER
            + progressIndex.getPeerId2SearchIndexSize() * (long) (Integer.BYTES + Long.BYTES);
    return size;
  }

  private long sizeOfMetaProgressIndex() {
    // Memory alignment of basic types and reference types in structures
    return RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + META_PROGRESS_INDEX_REFERENCE_SIZE
                + META_PROGRESS_INDEX_PRIMITIVE_SIZE)
        + REENTRANT_READ_WRITE_LOCK_SIZE;
  }

  private long sizeOfMinimumProgressIndex() {
    // Memory alignment of basic types and reference types in structures
    return RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER);
  }

  private long sizeOfRecoverProgressIndex(RecoverProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + RECOVER_PROGRESS_INDEX_REFERENCE_SIZE);

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    size +=
        NUM_BYTES_OBJECT_HEADER
            + progressIndex.getDataNodeId2LocalIndex().size() * (long) (Integer.BYTES + Long.BYTES);
    return size;
  }

  private long sizeOfSimpleProgressIndex() {
    // Memory alignment of basic types and reference types in structures
    return RamUsageEstimator.alignObjectSize(
        NUM_BYTES_OBJECT_HEADER
            + SIMPLE_PROGRESS_INDEX_REFERENCE_SIZE
            + SIMPLE_PROGRESS_INDEX_PRIMITIVE_SIZE);
  }

  private long sizeOfStateProgressIndex(StateProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER
                + STATE_PROGRESS_INDEX_REFERENCE_SIZE
                + STATE_PROGRESS_INDEX_PRIMITIVE_SIZE);

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    if (progressIndex.getState() != null) {
      size += NUM_BYTES_OBJECT_HEADER;
      for (Map.Entry<String, Binary> entry : progressIndex.getState().entrySet()) {
        size += sizeOfString(entry.getKey()) + sizeOfBinary(entry.getValue());
      }
    }
    return size;
  }

  private long sizeOfTimeWindowStateProgressIndex(TimeWindowStateProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + TIME_WINDOW_STATE_PROGRESS_INDEX_REFERENCE_SIZE);

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    if (progressIndex.getTimeSeries2TimestampWindowBufferPairMap() != null) {
      size += NUM_BYTES_OBJECT_HEADER;
      for (Map.Entry<String, Pair<Long, ByteBuffer>> entry :
          progressIndex.getTimeSeries2TimestampWindowBufferPairMap().entrySet()) {
        size += sizeOfString(entry.getKey()) + Long.BYTES * 2;
      }
    }
    return size;
  }

  // =============================Write==================================

  private long sizeOfBinary(Binary binary) {
    // -----header----
    // -----ref-------
    // ---------------
    // --arrayHeader--
    // ----values-----
    return RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF)
        + RamUsageEstimator.alignObjectSize(NUM_BYTES_ARRAY_HEADER + binary.getValues().length);
  }

  private long sizeOfString(String value) {
    // -----header----
    // -----ref-------
    // ---------------
    // --arrayHeader--
    // ----values-----
    return RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF)
        + RamUsageEstimator.alignObjectSize(NUM_BYTES_ARRAY_HEADER + value.length());
  }

  private long sizeOfStringArray(final String[] values) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + values.length * NUM_BYTES_OBJECT_REF);
    for (String value : values) {
      if (value == null) {
        continue;
      }
      size += sizeOfString(value);
    }
    return size;
  }

  private long sizeOfTimes(final long[] times) {
    long size = NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * times.length;
    return RamUsageEstimator.alignObjectSize(size);
  }

  private long sizeOfBitMapArray(BitMap[] bitMaps) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * bitMaps.length);
    for (BitMap bitMap : bitMaps) {
      size += sizeOfBitMap(bitMap);
    }
    return size;
  }

  private long sizeOfBitMap(BitMap bitMaps) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + BIT_MAP_REFERENCE_SIZE + BIT_MAP_PRIMITIVE_SIZE);

    size +=
        RamUsageEstimator.alignObjectSize(NUM_BYTES_ARRAY_HEADER + bitMaps.getByteArray().length);
    return size;
  }

  private long sizeOfValues(
      Object[] columns, MeasurementSchema[] measurementSchemas, BitMap[] bitMaps) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * columns.length);
    for (int i = 0; i < columns.length; i++) {
      switch (measurementSchemas[i].getType()) {
        case INT64:
        case TIMESTAMP:
          {
            final long[] values = (long[]) columns[i];
            size +=
                RamUsageEstimator.alignObjectSize(
                    NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * values.length);
            break;
          }
        case DATE:
        case INT32:
          {
            final int[] values = (int[]) columns[i];
            size +=
                RamUsageEstimator.alignObjectSize(
                    NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * values.length);
            break;
          }
        case DOUBLE:
          {
            final double[] values = (double[]) columns[i];
            size +=
                RamUsageEstimator.alignObjectSize(
                    NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * values.length);
            break;
          }
        case FLOAT:
          {
            final float[] values = (float[]) columns[i];
            size +=
                RamUsageEstimator.alignObjectSize(
                    NUM_BYTES_ARRAY_HEADER + (long) Float.BYTES * values.length);
            break;
          }
        case BOOLEAN:
          {
            final boolean[] values = (boolean[]) columns[i];
            size += RamUsageEstimator.alignObjectSize(NUM_BYTES_ARRAY_HEADER + values.length);
            break;
          }
        case STRING:
        case TEXT:
        case BLOB:
          {
            final Binary[] values = (Binary[]) columns[i];
            size +=
                RamUsageEstimator.alignObjectSize(
                    NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * values.length);
            if (bitMaps[i].isAllMarked()) {
              break;
            }
            int nullValueSize = 0;
            for (int j = 0; j < values.length; j++) {
              if (values[i] != null) {
                size += sizeOfBinary(values[i]) * (values.length - nullValueSize);
                break;
              }
              nullValueSize++;
            }
          }
      }
    }
    return size;
  }

  private long sizeOfValues(Object[] columns, MeasurementSchema[] measurementSchemas) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * columns.length);
    for (int i = 0; i < columns.length; i++) {
      switch (measurementSchemas[i].getType()) {
        case INT64:
        case TIMESTAMP:
          {
            size += Long.BYTES;
            break;
          }
        case DATE:
        case INT32:
          {
            size += Integer.BYTES;
            break;
          }
        case DOUBLE:
          {
            size += Double.BYTES;
            break;
          }
        case FLOAT:
          {
            size += Float.BYTES;
            break;
          }
        case BOOLEAN:
          {
            size += 1;
            break;
          }
        case STRING:
        case TEXT:
        case BLOB:
          {
            size += sizeOfBinary((Binary) columns[i]);
          }
      }
    }
    return size;
  }
}
