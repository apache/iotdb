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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
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
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InsertNodeMemoryEstimator {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertNodeMemoryEstimator.class);

  private static final String INSERT_TABLET_NODE = "InsertTabletNode";
  private static final String INSERT_ROW_NODE = "InsertRowNode";
  private static final String INSERT_ROWS_NODE = "InsertRowsNode";
  private static final String INSERT_ROWS_OF_ONE_DEVICE_NODE = "InsertRowsOfOneDeviceNode";
  private static final String INSERT_MULTI_TABLETS_NODE = "InsertMultiTabletsNode";
  private static final String RELATIONAL_INSERT_ROWS_NODE = "RelationalInsertRowsNode";
  private static final String RELATIONAL_INSERT_ROW_NODE = "RelationalInsertRowNode";
  private static final String RELATIONAL_INSERT_TABLET_NODE = "RelationalInsertTabletNode";

  private static final long NUM_BYTES_OBJECT_REF = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  private static final long NUM_BYTES_OBJECT_HEADER = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
  private static final long NUM_BYTES_ARRAY_HEADER = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

  private static final long REENTRANT_READ_WRITE_LOCK_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ReentrantReadWriteLock.class);

  private static final long TS_ENCODING_PLAIN_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.PLAIN));

  // =============================InsertNode==================================

  private static final long INSERT_TABLET_NODE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InsertTabletNode.class);

  private static final long INSERT_ROW_NODE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InsertRowNode.class);

  private static final long INSERT_ROWS_NODE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InsertRowsNode.class);

  private static final long INSERT_ROWS_OF_ONE_DEVICE_NODE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InsertRowsOfOneDeviceNode.class);

  private static final long INSERT_MULTI_TABLETS_NODE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InsertMultiTabletsNode.class);

  private static final long RELATIONAL_INSERT_ROWS_NODE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RelationalInsertRowsNode.class);

  private static final long RELATIONAL_INSERT_ROW_NODE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RelationalInsertRowNode.class);

  private static final long RELATIONAL_INSERT_TABLET_NODE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RelationalInsertTabletNode.class);

  // ============================Device And Measurement===================================

  private static final long PARTIAL_PATH_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PartialPath.class);

  private static final long MEASUREMENT_SCHEMA_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MeasurementSchema.class);

  private static final long PLAIN_DEVICE_ID_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PlainDeviceID.class);

  private static final long STRING_ARRAY_DEVICE_ID_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(StringArrayDeviceID.class);

  // =============================Thrift==================================

  private static final long T_REGION_REPLICA_SET_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TRegionReplicaSet.class);

  private static final long T_DATA_NODE_LOCATION_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TDataNodeLocation.class);

  private static final long TS_STATUS_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TSStatus.class);

  private static final long T_END_POINT_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TEndPoint.class);

  private static final long T_CONSENSUS_GROUP_ID_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TConsensusGroupId.class);

  // =============================ProgressIndex==================================

  private static final long HYBRID_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HybridProgressIndex.class);

  private static final long IOT_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(IoTProgressIndex.class);

  private static final long META_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MetaProgressIndex.class);

  private static final long RECOVER_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RecoverProgressIndex.class);

  private static final long SIMPLE_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SimpleProgressIndex.class);

  private static final long STATE_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(StateProgressIndex.class);

  private static final long TIME_WINDOW_STATE_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TimeWindowStateProgressIndex.class);

  // =============================BitMap==================================

  private static final long BIT_MAP_SIZE = RamUsageEstimator.shallowSizeOfInstance(BitMap.class);

  // ============================= Primitive Type Wrapper Classes =========

  private static final long SIZE_OF_LONG =
      RamUsageEstimator.alignObjectSize(Long.BYTES + NUM_BYTES_OBJECT_HEADER);
  private static final long SIZE_OF_INT =
      RamUsageEstimator.alignObjectSize(Integer.BYTES + NUM_BYTES_OBJECT_HEADER);
  private static final long SIZE_OF_DOUBLE =
      RamUsageEstimator.alignObjectSize(Double.BYTES + NUM_BYTES_OBJECT_HEADER);
  private static final long SIZE_OF_FLOAT =
      RamUsageEstimator.alignObjectSize(Float.BYTES + NUM_BYTES_OBJECT_HEADER);
  private static final long SIZE_OF_BOOLEAN =
      RamUsageEstimator.alignObjectSize(1 + NUM_BYTES_OBJECT_HEADER);
  private static final long SIZE_OF_SHORT =
      RamUsageEstimator.alignObjectSize(Short.BYTES + NUM_BYTES_OBJECT_HEADER);
  private static final long SIZE_OF_STRING = RamUsageEstimator.shallowSizeOfInstance(String.class);

  private static final long SIZE_OF_ARRAYLIST =
      RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);

  // The calculated result needs to be magnified by 1.3 times, which is 1.3 times different
  // from the actual result because the properties of the parent class are not added.
  private static final double INSERT_ROW_NODE_EXPANSION_FACTOR = 1.3;

  public static long sizeOf(final InsertNode insertNode) {
    try {
      final String className = insertNode.getClass().getSimpleName();
      switch (className) {
        case INSERT_TABLET_NODE:
          return sizeOfInsertTabletNode((InsertTabletNode) insertNode);
        case INSERT_ROW_NODE:
          return (long)
              (sizeOfInsertRowNode((InsertRowNode) insertNode) * INSERT_ROW_NODE_EXPANSION_FACTOR);
        case INSERT_ROWS_NODE:
          return sizeOfInsertRowsNode((InsertRowsNode) insertNode);
        case INSERT_ROWS_OF_ONE_DEVICE_NODE:
          return sizeOfInsertRowsOfOneDeviceNode((InsertRowsOfOneDeviceNode) insertNode);
        case INSERT_MULTI_TABLETS_NODE:
          return sizeOfInsertMultiTabletsNode((InsertMultiTabletsNode) insertNode);
        case RELATIONAL_INSERT_ROWS_NODE:
          return sizeOfRelationalInsertRowsNode((RelationalInsertRowsNode) insertNode);
        case RELATIONAL_INSERT_ROW_NODE:
          return sizeOfRelationalInsertRowNode((RelationalInsertRowNode) insertNode);
        case RELATIONAL_INSERT_TABLET_NODE:
          return sizeOfRelationalInsertTabletNode((RelationalInsertTabletNode) insertNode);
        default:
          return 0L;
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to estimate size for InsertNode: {}", e.getMessage(), e);
      return 0L;
    }
  }

  // =============================InsertNode==================================

  private static long calculateFullInsertNodeSize(final InsertNode node) {
    long size = 0;
    // PartialPath
    size += sizeOfPartialPath(node.getTargetPath());
    // MeasurementSchemas
    size += sizeOfMeasurementSchemas(node.getMeasurementSchemas());
    // Measurement
    size += sizeOfMeasurement(node.getMeasurements());
    // dataTypes
    size += RamUsageEstimator.shallowSizeOf(node.getDataTypes());
    // columnCategories
    size += RamUsageEstimator.shallowSizeOf(node.getColumnCategories());
    // idColumnIndices
    size += sizeOfColumnIndices(node.getColumnCategories());
    // deviceID
    if (node.isDeviceIDExists()) {
      size += sizeOfIDeviceID(node.getDeviceID());
    }
    // dataRegionReplicaSet
    size += sizeOfTRegionReplicaSet(node.getRegionReplicaSet());
    // progressIndex
    size += sizeOfProgressIndex(node.getProgressIndex());
    return size;
  }

  private static long calculateInsertNodeSizeExcludingSchemas(final InsertNode node) {
    // Measurement
    long size = 2 * RamUsageEstimator.shallowSizeOf(node.getMeasurementSchemas());
    // dataTypes
    size += RamUsageEstimator.shallowSizeOf(node.getDataTypes());
    // columnCategories
    size += RamUsageEstimator.shallowSizeOf(node.getColumnCategories());
    // idColumnIndices
    size += sizeOfColumnIndices(node.getColumnCategories());
    // deviceID
    if (node.isDeviceIDExists()) {
      size += sizeOfIDeviceID(node.getDeviceID());
    }
    // dataRegionReplicaSet
    size += sizeOfTRegionReplicaSet(node.getRegionReplicaSet());
    // progressIndex
    size += sizeOfProgressIndex(node.getProgressIndex());
    return size;
  }

  private static long sizeOfInsertTabletNode(final InsertTabletNode node) {
    long size = INSERT_TABLET_NODE_SIZE;
    size += calculateFullInsertNodeSize(node);
    size += sizeOfTimes(node.getTimes());
    size += sizeOfBitMapArray(node.getBitMaps());
    size += sizeOfColumns(node.getColumns(), node.getMeasurementSchemas());
    final List<Integer> range = node.getRange();
    if (range != null) {
      size += NUM_BYTES_OBJECT_HEADER + SIZE_OF_INT * range.size();
    }
    return size;
  }

  private static long calculateInsertTabletNodeSizeExcludingSchemas(final InsertTabletNode node) {
    long size = INSERT_TABLET_NODE_SIZE;

    size += calculateInsertNodeSizeExcludingSchemas(node);

    size += sizeOfTimes(node.getTimes());

    size += sizeOfBitMapArray(node.getBitMaps());

    size += sizeOfColumns(node.getColumns(), node.getMeasurementSchemas());

    final List<Integer> range = node.getRange();
    if (range != null) {
      size += NUM_BYTES_OBJECT_HEADER + SIZE_OF_INT * range.size();
    }
    return size;
  }

  private static long sizeOfInsertRowNode(final InsertRowNode node) {
    long size = INSERT_ROW_NODE_SIZE;
    size += calculateFullInsertNodeSize(node);
    size += sizeOfValues(node.getValues(), node.getMeasurementSchemas());
    return size;
  }

  private static long calculateInsertRowNodeExcludingSchemas(final InsertRowNode node) {
    long size = INSERT_ROW_NODE_SIZE;
    size += calculateInsertNodeSizeExcludingSchemas(node);
    size += sizeOfValues(node.getValues(), node.getMeasurementSchemas());
    return size;
  }

  private static long sizeOfInsertRowsNode(final InsertRowsNode node) {
    long size = INSERT_ROWS_NODE_SIZE;
    size += calculateFullInsertNodeSize(node);
    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      // InsertRowNodeList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (calculateInsertRowNodeExcludingSchemas(rows.get(0)) + NUM_BYTES_OBJECT_REF)
              * rows.size();
      size += sizeOfPartialPath(rows.get(0).getTargetPath());
      size += sizeOfMeasurementSchemas(rows.get(0).getMeasurementSchemas());
      // InsertRowNodeIndexList
      size += NUM_BYTES_OBJECT_HEADER;
      size += (long) indexList.size() * (SIZE_OF_INT + NUM_BYTES_OBJECT_REF);
    }
    return size;
  }

  private static long sizeOfInsertRowsOfOneDeviceNode(final InsertRowsOfOneDeviceNode node) {
    long size = INSERT_ROWS_OF_ONE_DEVICE_NODE_SIZE;
    size += calculateFullInsertNodeSize(node);
    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      // InsertRowNodeList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (calculateInsertRowNodeExcludingSchemas(rows.get(0)) + NUM_BYTES_OBJECT_REF)
              * rows.size();
      size += sizeOfPartialPath(rows.get(0).getTargetPath());
      size += sizeOfMeasurementSchemas(rows.get(0).getMeasurementSchemas());
      // InsertRowNodeIndexList
      size += NUM_BYTES_OBJECT_HEADER;
      size += (long) indexList.size() * (SIZE_OF_INT + NUM_BYTES_OBJECT_REF);
    }
    // results
    size += NUM_BYTES_OBJECT_HEADER;
    for (Map.Entry<Integer, TSStatus> entry : node.getResults().entrySet()) {
      size +=
          Integer.BYTES
              + sizeOfTSStatus(entry.getValue())
              + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
    }
    return size;
  }

  private static long sizeOfInsertMultiTabletsNode(final InsertMultiTabletsNode node) {
    long size = INSERT_MULTI_TABLETS_NODE_SIZE;
    size += calculateFullInsertNodeSize(node);
    // dataTypes
    size += RamUsageEstimator.shallowSizeOf(node.getDataTypes());
    // columnCategories
    size += RamUsageEstimator.shallowSizeOf(node.getColumnCategories());

    final List<InsertTabletNode> rows = node.getInsertTabletNodeList();
    final List<Integer> indexList = node.getParentInsertTabletNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      // InsertTabletNodeList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (calculateInsertTabletNodeSizeExcludingSchemas(rows.get(0)) + NUM_BYTES_OBJECT_REF)
              * rows.size();
      size += sizeOfPartialPath(rows.get(0).getTargetPath());
      size += sizeOfMeasurementSchemas(rows.get(0).getMeasurementSchemas());
      // ParentInsertTabletNodeIndexList
      size += NUM_BYTES_OBJECT_HEADER;
      size += (long) indexList.size() * (SIZE_OF_INT + NUM_BYTES_OBJECT_REF);
    }
    // results
    if (node.getResults() != null) {
      size += NUM_BYTES_OBJECT_HEADER;
      for (Map.Entry<Integer, TSStatus> entry : node.getResults().entrySet()) {
        size +=
            Integer.BYTES
                + sizeOfTSStatus(entry.getValue())
                + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      }
    }
    return size;
  }

  private static long sizeOfRelationalInsertRowsNode(final RelationalInsertRowsNode node) {
    long size = RELATIONAL_INSERT_ROWS_NODE_SIZE;
    size += calculateFullInsertNodeSize(node);
    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      // InsertRowNodeList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (calculateInsertRowNodeExcludingSchemas(rows.get(0)) + NUM_BYTES_OBJECT_REF)
              * rows.size();
      size += sizeOfPartialPath(rows.get(0).getTargetPath());
      size += sizeOfMeasurementSchemas(rows.get(0).getMeasurementSchemas());
      // InsertRowNodeIndexList
      size += NUM_BYTES_OBJECT_HEADER;
      size += (long) indexList.size() * (SIZE_OF_INT + NUM_BYTES_OBJECT_REF);
    }
    // ignore deviceIDs
    return size;
  }

  private static long sizeOfRelationalInsertRowNode(final RelationalInsertRowNode node) {
    long size = RELATIONAL_INSERT_ROW_NODE_SIZE;
    size += calculateFullInsertNodeSize(node);
    size += sizeOfValues(node.getValues(), node.getMeasurementSchemas());
    return size;
  }

  private static long sizeOfRelationalInsertTabletNode(final RelationalInsertTabletNode node) {
    long size = RELATIONAL_INSERT_TABLET_NODE_SIZE;

    size += calculateFullInsertNodeSize(node);

    size += sizeOfTimes(node.getTimes());

    size += sizeOfBitMapArray(node.getBitMaps());

    size += sizeOfColumns(node.getColumns(), node.getMeasurementSchemas());

    final List<Integer> range = node.getRange();
    if (range != null) {
      size += NUM_BYTES_OBJECT_HEADER + (NUM_BYTES_OBJECT_REF + Integer.BYTES) * range.size();
    }
    // ignore deviceIDs
    return size;
  }

  // ============================Device And Measurement===================================

  private static long sizeOfPartialPath(final PartialPath partialPath) {
    if (partialPath == null) {
      return 0L;
    }
    long size = PARTIAL_PATH_SIZE;
    final String[] nodes = partialPath.getNodes();
    if (nodes != null) {
      // Since fullPath may be lazy loaded, lazy loading will not be triggered here, so it is
      // assumed that the memory size of fullPath is the same as that of nodes.
      size += sizeOfStringArray(nodes) * 2;
      size += TsFileConstant.PATH_SEPARATOR.length() * (nodes.length - 1) + NUM_BYTES_OBJECT_HEADER;
    }
    return size;
  }

  private static long sizeOfMeasurementSchemas(final MeasurementSchema[] measurementSchemas) {
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

  private static long sizeOfMeasurementSchema(final MeasurementSchema measurementSchema) {
    if (measurementSchema == null) {
      return 0L;
    }
    // Header + primitive + reference
    long size = MEASUREMENT_SCHEMA_SIZE;
    // measurementId
    size += sizeOfString(measurementSchema.getMeasurementId());
    // props
    final Map<String, String> props = measurementSchema.getProps();
    if (props != null) {
      size += NUM_BYTES_OBJECT_HEADER;
      for (Map.Entry<String, String> entry : props.entrySet()) {
        size +=
            sizeOfString(entry.getKey())
                + sizeOfString(entry.getValue())
                + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      }
    }
    size += TS_ENCODING_PLAIN_BUILDER_SIZE;
    return size;
  }

  private static long sizeOfMeasurement(final String[] measurement) {
    if (measurement == null) {
      return 0L;
    }
    return sizeOfStringArray(measurement);
  }

  private static long sizeOfColumnIndices(final TsTableColumnCategory[] columnCategories) {
    if (columnCategories == null) {
      return 0L;
    }
    // ArrayList<Integer>
    long size = SIZE_OF_ARRAYLIST;
    size += NUM_BYTES_ARRAY_HEADER;
    for (TsTableColumnCategory columnCategory : columnCategories) {
      if (columnCategory != null && columnCategory.equals(TsTableColumnCategory.ID)) {
        size += SIZE_OF_INT + NUM_BYTES_OBJECT_REF;
      }
    }
    return size;
  }

  private static long sizeOfIDeviceID(final IDeviceID deviceID) {
    if (deviceID == null) {
      return 0L;
    }
    if (deviceID instanceof PlainDeviceID) {
      return sizeOfPlainDeviceID((PlainDeviceID) deviceID);
    }
    return sizeOfStringArrayDeviceID((StringArrayDeviceID) deviceID);
  }

  private static long sizeOfPlainDeviceID(final PlainDeviceID deviceID) {
    long size = PLAIN_DEVICE_ID_SIZE;
    final String id = deviceID.toString();

    if (id != null) {
      // Estimate the sum of the table and segment lengths to be the size of the id
      size += sizeOfString(id) * 2;
    }

    return size;
  }

  private static long sizeOfStringArrayDeviceID(final StringArrayDeviceID deviceID) {
    // Memory alignment of basic types and reference types in structures
    long size = STRING_ARRAY_DEVICE_ID_SIZE;
    size += sizeOfStringArray(deviceID.getSegments());
    return size;
  }

  // =============================Thrift==================================

  private static long sizeOfTRegionReplicaSet(final TRegionReplicaSet tRegionReplicaSet) {
    if (tRegionReplicaSet == null) {
      return 0L;
    }
    // Memory alignment of basic types and reference types in structures
    long size = T_REGION_REPLICA_SET_SIZE;
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

  private static long sizeOfTConsensusGroupId() {
    // objectHeader + type + id
    return T_CONSENSUS_GROUP_ID_SIZE;
  }

  private static long sizeOfTDataNodeLocation(final TDataNodeLocation tDataNodeLocation) {
    if (tDataNodeLocation == null) {
      return 0L;
    }
    long size = T_DATA_NODE_LOCATION_SIZE;

    size += sizeOfTEndPoint(tDataNodeLocation.getClientRpcEndPoint());
    size += sizeOfTEndPoint(tDataNodeLocation.getInternalEndPoint());
    size += sizeOfTEndPoint(tDataNodeLocation.getMPPDataExchangeEndPoint());
    size += sizeOfTEndPoint(tDataNodeLocation.getDataRegionConsensusEndPoint());
    size += sizeOfTEndPoint(tDataNodeLocation.getSchemaRegionConsensusEndPoint());

    return size;
  }

  private static long sizeOfTEndPoint(final TEndPoint tEndPoint) {
    if (tEndPoint == null) {
      return 0L;
    }
    // objectHeader + ip + port
    long size = T_END_POINT_SIZE;

    size += sizeOfString(tEndPoint.ip);
    return size;
  }

  private static long sizeOfTSStatus(final TSStatus tSStatus) {
    if (tSStatus == null) {
      return 0L;
    }
    long size = TS_STATUS_SIZE;
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

  private static long sizeOfProgressIndex(final ProgressIndex progressIndex) {
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
        return 0L;
      case RECOVER_PROGRESS_INDEX:
        return sizeOfRecoverProgressIndex((RecoverProgressIndex) progressIndex);
      case TIME_WINDOW_STATE_PROGRESS_INDEX:
        return sizeOfTimeWindowStateProgressIndex((TimeWindowStateProgressIndex) progressIndex);
    }
    return 0L;
  }

  private static long sizeOfHybridProgressIndex(final HybridProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size = HYBRID_PROGRESS_INDEX_SIZE;

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    if (progressIndex.getType2Index() != null) {
      // ignore ProgressIndex
      size +=
          NUM_BYTES_OBJECT_HEADER
              + progressIndex.getType2Index().size()
                  * (SIZE_OF_SHORT + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
    }
    return size;
  }

  private static long sizeOfIoTProgressIndex(IoTProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size = IOT_PROGRESS_INDEX_SIZE;

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;

    size +=
        NUM_BYTES_OBJECT_HEADER
            + progressIndex.getPeerId2SearchIndexSize()
                * (SIZE_OF_INT + SIZE_OF_LONG + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);

    return size;
  }

  private static long sizeOfMetaProgressIndex() {
    // Memory alignment of basic types and reference types in structures
    return META_PROGRESS_INDEX_SIZE + REENTRANT_READ_WRITE_LOCK_SIZE;
  }

  private static long sizeOfRecoverProgressIndex(RecoverProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size = RECOVER_PROGRESS_INDEX_SIZE;

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    if (progressIndex.getDataNodeId2LocalIndex() != null) {
      size +=
          NUM_BYTES_OBJECT_HEADER
              + progressIndex.getDataNodeId2LocalIndex().size()
                  * (SIZE_OF_INT + SIZE_OF_LONG + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
    }
    return size;
  }

  private static long sizeOfSimpleProgressIndex() {
    // Memory alignment of basic types and reference types in structures
    return SIMPLE_PROGRESS_INDEX_SIZE;
  }

  private static long sizeOfStateProgressIndex(StateProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size = STATE_PROGRESS_INDEX_SIZE;

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    if (progressIndex.getState() != null) {
      size += NUM_BYTES_OBJECT_HEADER;
      for (Map.Entry<String, Binary> entry : progressIndex.getState().entrySet()) {
        size +=
            RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                + sizeOfString(entry.getKey())
                + sizeOfBinary(entry.getValue());
      }
    }
    return size;
  }

  private static long sizeOfTimeWindowStateProgressIndex(
      TimeWindowStateProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size = TIME_WINDOW_STATE_PROGRESS_INDEX_SIZE;

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;
    if (progressIndex.getTimeSeries2TimestampWindowBufferPairMap() != null) {
      size += NUM_BYTES_OBJECT_HEADER;
      for (Map.Entry<String, Pair<Long, ByteBuffer>> entry :
          progressIndex.getTimeSeries2TimestampWindowBufferPairMap().entrySet()) {
        size +=
            sizeOfString(entry.getKey())
                + SIZE_OF_LONG * 2
                + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      }
    }
    return size;
  }

  // =============================Write==================================

  private static long sizeOfBinary(Binary binary) {
    if (binary == null) {
      return 0;
    }
    // -----header----
    // -----ref-------
    // ---------------
    // --arrayHeader--
    // ----values-----
    return RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF)
        + RamUsageEstimator.alignObjectSize(NUM_BYTES_ARRAY_HEADER + binary.getValues().length);
  }

  private static long sizeOfString(String value) {
    if (value == null) {
      return 0;
    }
    // -----header----
    // -----ref-------
    // ---------------
    // --arrayHeader--
    // ----values-----
    return SIZE_OF_STRING
        + RamUsageEstimator.alignObjectSize(NUM_BYTES_ARRAY_HEADER + value.length());
  }

  private static long sizeOfStringArray(final String[] values) {
    if (values == null) {
      return 0;
    }
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + values.length * NUM_BYTES_OBJECT_REF);
    for (String value : values) {
      size += sizeOfString(value);
    }
    return size;
  }

  private static long sizeOfTimes(final long[] times) {
    if (times == null) {
      return 0;
    }
    long size = NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * times.length;
    return RamUsageEstimator.alignObjectSize(size);
  }

  private static long sizeOfBitMapArray(BitMap[] bitMaps) {
    if (bitMaps == null) {
      return 0L;
    }
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * bitMaps.length);
    for (BitMap bitMap : bitMaps) {
      size += sizeOfBitMap(bitMap);
    }
    return size;
  }

  private static long sizeOfBitMap(final BitMap bitMaps) {
    if (bitMaps == null) {
      return 0L;
    }
    long size = BIT_MAP_SIZE;

    size +=
        RamUsageEstimator.alignObjectSize(NUM_BYTES_ARRAY_HEADER + bitMaps.getByteArray().length);
    return size;
  }

  private static long sizeOfColumns(
      final Object[] columns, final MeasurementSchema[] measurementSchemas) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * columns.length);
    for (int i = 0; i < columns.length; i++) {
      switch (measurementSchemas[i].getType()) {
        case INT64:
        case TIMESTAMP:
          {
            size += RamUsageEstimator.sizeOf((long[]) columns[i]);
            break;
          }
        case DATE:
        case INT32:
          {
            size += RamUsageEstimator.sizeOf((int[]) columns[i]);
            break;
          }
        case DOUBLE:
          {
            size += RamUsageEstimator.sizeOf((double[]) columns[i]);
            break;
          }
        case FLOAT:
          {
            size += RamUsageEstimator.sizeOf((float[]) columns[i]);
            break;
          }
        case BOOLEAN:
          {
            size += RamUsageEstimator.sizeOf((boolean[]) columns[i]);
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
            for (Binary value : values) {
              size += sizeOfBinary(value);
            }
            break;
          }
      }
    }
    return size;
  }

  private static long sizeOfValues(
      final Object[] columns, final MeasurementSchema[] measurementSchemas) {
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * columns.length);
    for (int i = 0; i < columns.length; i++) {
      switch (measurementSchemas[i].getType()) {
        case INT64:
        case TIMESTAMP:
          {
            size += SIZE_OF_LONG;
            break;
          }
        case DATE:
        case INT32:
          {
            size += SIZE_OF_INT;
            break;
          }
        case DOUBLE:
          {
            size += SIZE_OF_DOUBLE;
            break;
          }
        case FLOAT:
          {
            size += SIZE_OF_FLOAT;
            break;
          }
        case BOOLEAN:
          {
            size += SIZE_OF_BOOLEAN;
            break;
          }
        case STRING:
        case TEXT:
        case BLOB:
          {
            final Binary binary = (Binary) columns[i];
            size += sizeOfBinary(binary);
          }
      }
    }
    return size;
  }
}
