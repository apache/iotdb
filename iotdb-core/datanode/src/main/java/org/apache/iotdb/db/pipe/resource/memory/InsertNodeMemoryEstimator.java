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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
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
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
      LOGGER.warn(DataNodePipeMessages.FAILED_TO_ESTIMATE_SIZE_FOR_INSERTNODE, e.getMessage(), e);
      return 0L;
    }
  }

  // =============================InsertNode==================================

  private static long calculateFullInsertNodeSize(final InsertNode node) {
    return calculateFullInsertNodeSize(node, null);
  }

  private static long calculateFullInsertNodeSize(
      final InsertNode node, final Set<Object> deduplicatedObjects) {
    long size = 0;
    // PlanNodeId
    size += sizeOfPlanNodeId(node.getPlanNodeId(), deduplicatedObjects);
    // PartialPath
    size += sizeOfPartialPath(node.getTargetPath(), deduplicatedObjects);
    // MeasurementSchemas
    size += sizeOfMeasurementSchemas(node.getMeasurementSchemas(), deduplicatedObjects);
    // Measurement
    size += sizeOfStringArray(node.getMeasurements(), deduplicatedObjects);
    // dataTypes
    size += sizeOfShallowObject(node.getDataTypes(), deduplicatedObjects);
    // columnCategories
    size += sizeOfShallowObject(node.getColumnCategories(), deduplicatedObjects);
    // idColumnIndices
    size += sizeOfColumnIndices(node.getColumnCategories());
    // deviceID
    if (node.isDeviceIDExists()) {
      size += sizeOfIDeviceID(node.getDeviceID(), deduplicatedObjects);
    }
    // dataRegionReplicaSet
    size += sizeOfTRegionReplicaSet(node.getRegionReplicaSet(), deduplicatedObjects);
    // progressIndex
    size += sizeOfProgressIndex(node.getProgressIndex(), deduplicatedObjects);
    return size;
  }

  private static long sizeOfInsertTabletNode(final InsertTabletNode node) {
    return sizeOfInsertTabletNode(node, newDeduplicatedObjectSet());
  }

  private static long sizeOfInsertTabletNode(
      final InsertTabletNode node, final Set<Object> deduplicatedObjects) {
    long size = INSERT_TABLET_NODE_SIZE;
    size += calculateFullInsertNodeSize(node, deduplicatedObjects);
    size += RamUsageEstimator.sizeOf(node.getTimes());
    size += RamUsageEstimator.sizeOf(node.getBitMaps());
    size += sizeOfColumns(node.getColumns(), node.getMeasurementSchemas());
    size += sizeOfIntegerList(node.getRange());
    return size;
  }

  private static long sizeOfInsertRowNode(final InsertRowNode node) {
    return sizeOfInsertRowNode(node, newDeduplicatedObjectSet());
  }

  private static long sizeOfInsertRowNode(
      final InsertRowNode node, final Set<Object> deduplicatedObjects) {
    long size = INSERT_ROW_NODE_SIZE;
    size += calculateFullInsertNodeSize(node, deduplicatedObjects);
    size += sizeOfValues(node.getValues(), node.getMeasurementSchemas());
    return size;
  }

  private static long sizeOfInsertRowsNode(final InsertRowsNode node) {
    final Set<Object> deduplicatedObjects = newDeduplicatedObjectSet();
    long size = INSERT_ROWS_NODE_SIZE;
    size += calculateFullInsertNodeSize(node, deduplicatedObjects);
    size += sizeOfInsertRowNodeList(node.getInsertRowNodeList(), deduplicatedObjects);
    size += sizeOfIntegerList(node.getInsertRowNodeIndexList());
    size += sizeOfResults(node.getResults());
    return size;
  }

  private static long sizeOfInsertRowsOfOneDeviceNode(final InsertRowsOfOneDeviceNode node) {
    final Set<Object> deduplicatedObjects = newDeduplicatedObjectSet();
    long size = INSERT_ROWS_OF_ONE_DEVICE_NODE_SIZE;
    size += calculateFullInsertNodeSize(node, deduplicatedObjects);
    size += sizeOfInsertRowNodeList(node.getInsertRowNodeList(), deduplicatedObjects);
    size += sizeOfIntegerList(node.getInsertRowNodeIndexList());
    size += sizeOfResults(node.getResults());
    return size;
  }

  private static long sizeOfInsertMultiTabletsNode(final InsertMultiTabletsNode node) {
    final Set<Object> deduplicatedObjects = newDeduplicatedObjectSet();
    long size = INSERT_MULTI_TABLETS_NODE_SIZE;
    size += calculateFullInsertNodeSize(node, deduplicatedObjects);
    size += sizeOfInsertTabletNodeList(node.getInsertTabletNodeList(), deduplicatedObjects);
    size += sizeOfIntegerList(node.getParentInsertTabletNodeIndexList());
    size += sizeOfResults(node.getResults());
    return size;
  }

  private static long sizeOfRelationalInsertRowsNode(final RelationalInsertRowsNode node) {
    final Set<Object> deduplicatedObjects = newDeduplicatedObjectSet();
    long size = RELATIONAL_INSERT_ROWS_NODE_SIZE;
    size += calculateFullInsertNodeSize(node, deduplicatedObjects);
    size += sizeOfInsertRowNodeList(node.getInsertRowNodeList(), deduplicatedObjects);
    size += sizeOfIntegerList(node.getInsertRowNodeIndexList());
    // ignore deviceIDs
    return size;
  }

  private static long sizeOfRelationalInsertRowNode(final RelationalInsertRowNode node) {
    return sizeOfRelationalInsertRowNode(node, newDeduplicatedObjectSet());
  }

  private static long sizeOfRelationalInsertRowNode(
      final RelationalInsertRowNode node, final Set<Object> deduplicatedObjects) {
    long size = RELATIONAL_INSERT_ROW_NODE_SIZE;
    size += calculateFullInsertNodeSize(node, deduplicatedObjects);
    size += sizeOfValues(node.getValues(), node.getMeasurementSchemas());
    return size;
  }

  private static long sizeOfRelationalInsertTabletNode(final RelationalInsertTabletNode node) {
    return sizeOfRelationalInsertTabletNode(node, newDeduplicatedObjectSet());
  }

  private static long sizeOfRelationalInsertTabletNode(
      final RelationalInsertTabletNode node, final Set<Object> deduplicatedObjects) {
    long size = RELATIONAL_INSERT_TABLET_NODE_SIZE;

    size += calculateFullInsertNodeSize(node, deduplicatedObjects);

    size += RamUsageEstimator.sizeOf(node.getTimes());

    size += RamUsageEstimator.sizeOf(node.getBitMaps());

    size += sizeOfColumns(node.getColumns(), node.getMeasurementSchemas());

    size += sizeOfIntegerList(node.getRange());
    // ignore deviceIDs
    return size;
  }

  // ============================Device And Measurement===================================

  public static long sizeOfPartialPath(final PartialPath partialPath) {
    return sizeOfPartialPath(partialPath, null);
  }

  private static long sizeOfPartialPath(
      final PartialPath partialPath, final Set<Object> deduplicatedObjects) {
    if (partialPath == null) {
      return 0L;
    }
    if (!shouldCountObject(partialPath, deduplicatedObjects)) {
      return 0L;
    }
    long size = PARTIAL_PATH_SIZE;
    final String[] nodes = partialPath.getNodes();
    if (nodes != null) {
      // Since fullPath may be lazy loaded, lazy loading will not be triggered here, so it is
      // assumed that the memory size of fullPath is the same as that of nodes.
      size += RamUsageEstimator.sizeOf(nodes) * 2;
      size += TsFileConstant.PATH_SEPARATOR.length() * (nodes.length - 1) + NUM_BYTES_OBJECT_HEADER;
    }
    return size;
  }

  public static long sizeOfMeasurementSchemas(final MeasurementSchema[] measurementSchemas) {
    return sizeOfMeasurementSchemas(measurementSchemas, null);
  }

  private static long sizeOfMeasurementSchemas(
      final MeasurementSchema[] measurementSchemas, final Set<Object> deduplicatedObjects) {
    if (measurementSchemas == null) {
      return 0L;
    }
    if (!shouldCountObject(measurementSchemas, deduplicatedObjects)) {
      return 0L;
    }
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * measurementSchemas.length);
    for (MeasurementSchema measurementSchema : measurementSchemas) {
      size += sizeOfMeasurementSchema(measurementSchema, deduplicatedObjects);
    }
    return size;
  }

  public static long sizeOfMeasurementSchema(final MeasurementSchema measurementSchema) {
    return sizeOfMeasurementSchema(measurementSchema, null);
  }

  private static long sizeOfMeasurementSchema(
      final MeasurementSchema measurementSchema, final Set<Object> deduplicatedObjects) {
    if (measurementSchema == null) {
      return 0L;
    }
    if (!shouldCountObject(measurementSchema, deduplicatedObjects)) {
      return 0L;
    }
    // Header + primitive + reference
    long size = MEASUREMENT_SCHEMA_SIZE;
    // measurementId
    size += RamUsageEstimator.sizeOf(measurementSchema.getMeasurementName());
    // props
    final Map<String, String> props = measurementSchema.getProps();
    if (props != null) {
      size += RamUsageEstimator.shallowSizeOf(props);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        size +=
            RamUsageEstimator.sizeOf(entry.getKey())
                + RamUsageEstimator.sizeOf(entry.getValue())
                + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      }
    }
    size += TS_ENCODING_PLAIN_BUILDER_SIZE;
    return size;
  }

  private static long sizeOfColumnIndices(final TsTableColumnCategory[] columnCategories) {
    if (columnCategories == null) {
      return 0L;
    }
    // ArrayList<Integer>
    long size = SIZE_OF_ARRAYLIST;
    size += NUM_BYTES_ARRAY_HEADER;
    for (TsTableColumnCategory columnCategory : columnCategories) {
      if (columnCategory != null && columnCategory.equals(TsTableColumnCategory.TAG)) {
        size += SIZE_OF_INT + NUM_BYTES_OBJECT_REF;
      }
    }
    return size;
  }

  public static long sizeOfIDeviceID(final IDeviceID deviceID) {
    return sizeOfIDeviceID(deviceID, null);
  }

  private static long sizeOfIDeviceID(
      final IDeviceID deviceID, final Set<Object> deduplicatedObjects) {
    return Objects.nonNull(deviceID) && shouldCountObject(deviceID, deduplicatedObjects)
        ? deviceID.ramBytesUsed()
        : 0L;
  }

  // =============================Thrift==================================

  private static long sizeOfTRegionReplicaSet(final TRegionReplicaSet tRegionReplicaSet) {
    return sizeOfTRegionReplicaSet(tRegionReplicaSet, null);
  }

  private static long sizeOfTRegionReplicaSet(
      final TRegionReplicaSet tRegionReplicaSet, final Set<Object> deduplicatedObjects) {
    if (tRegionReplicaSet == null) {
      return 0L;
    }
    if (!shouldCountObject(tRegionReplicaSet, deduplicatedObjects)) {
      return 0L;
    }
    // Memory alignment of basic types and reference types in structures
    long size = T_REGION_REPLICA_SET_SIZE;
    // Memory calculation in reference type, cannot get exact value, roughly estimate
    if (tRegionReplicaSet.isSetRegionId()) {
      size += sizeOfTConsensusGroupId();
    }
    if (tRegionReplicaSet.isSetDataNodeLocations()) {
      size += sizeOfObjectList(tRegionReplicaSet.getDataNodeLocations());
      for (TDataNodeLocation tDataNodeLocation : tRegionReplicaSet.getDataNodeLocations()) {
        size += sizeOfTDataNodeLocation(tDataNodeLocation, deduplicatedObjects);
      }
    }
    return size;
  }

  private static long sizeOfTConsensusGroupId() {
    // objectHeader + type + id
    return T_CONSENSUS_GROUP_ID_SIZE;
  }

  private static long sizeOfTDataNodeLocation(
      final TDataNodeLocation tDataNodeLocation, final Set<Object> deduplicatedObjects) {
    if (tDataNodeLocation == null) {
      return 0L;
    }
    if (!shouldCountObject(tDataNodeLocation, deduplicatedObjects)) {
      return 0L;
    }
    long size = T_DATA_NODE_LOCATION_SIZE;

    size += sizeOfTEndPoint(tDataNodeLocation.getClientRpcEndPoint(), deduplicatedObjects);
    size += sizeOfTEndPoint(tDataNodeLocation.getInternalEndPoint(), deduplicatedObjects);
    size += sizeOfTEndPoint(tDataNodeLocation.getMPPDataExchangeEndPoint(), deduplicatedObjects);
    size +=
        sizeOfTEndPoint(tDataNodeLocation.getDataRegionConsensusEndPoint(), deduplicatedObjects);
    size +=
        sizeOfTEndPoint(tDataNodeLocation.getSchemaRegionConsensusEndPoint(), deduplicatedObjects);

    return size;
  }

  private static long sizeOfTEndPoint(final TEndPoint tEndPoint) {
    return sizeOfTEndPoint(tEndPoint, null);
  }

  private static long sizeOfTEndPoint(
      final TEndPoint tEndPoint, final Set<Object> deduplicatedObjects) {
    if (tEndPoint == null) {
      return 0L;
    }
    if (!shouldCountObject(tEndPoint, deduplicatedObjects)) {
      return 0L;
    }
    // objectHeader + ip + port
    long size = T_END_POINT_SIZE;

    size += RamUsageEstimator.sizeOf(tEndPoint.ip);
    return size;
  }

  private static long sizeOfTSStatus(final TSStatus tSStatus) {
    return sizeOfTSStatus(tSStatus, null);
  }

  private static long sizeOfTSStatus(
      final TSStatus tSStatus, final Set<Object> deduplicatedObjects) {
    if (tSStatus == null) {
      return 0L;
    }
    if (!shouldCountObject(tSStatus, deduplicatedObjects)) {
      return 0L;
    }
    long size = TS_STATUS_SIZE;
    // message
    if (tSStatus.isSetMessage()) {
      size += RamUsageEstimator.sizeOf(tSStatus.message);
    }
    // subStatus
    if (tSStatus.getSubStatus() != null) {
      size += sizeOfObjectList(tSStatus.getSubStatus());
      for (TSStatus subStatus : tSStatus.getSubStatus()) {
        size += sizeOfTSStatus(subStatus, deduplicatedObjects);
      }
    }
    // redirectNode
    if (tSStatus.isSetRedirectNode()) {
      size += sizeOfTEndPoint(tSStatus.redirectNode, deduplicatedObjects);
    }
    return size;
  }

  // =============================ProgressIndex==================================

  private static long sizeOfProgressIndex(final ProgressIndex progressIndex) {
    return sizeOfProgressIndex(progressIndex, null);
  }

  private static long sizeOfProgressIndex(
      final ProgressIndex progressIndex, final Set<Object> deduplicatedObjects) {
    return Objects.nonNull(progressIndex) && shouldCountObject(progressIndex, deduplicatedObjects)
        ? progressIndex.ramBytesUsed()
        : 0L;
  }

  // =============================Write==================================

  private static long sizeOfBinary(final Binary binary) {
    return Objects.nonNull(binary) ? binary.ramBytesUsed() : 0L;
  }

  public static long sizeOfColumns(
      final Object[] columns, final MeasurementSchema[] measurementSchemas) {
    if (Objects.isNull(columns)) {
      return 0L;
    }
    // Directly calculate if measurementSchemas are absent
    if (Objects.isNull(measurementSchemas)) {
      return RamUsageEstimator.shallowSizeOf(columns)
          + Arrays.stream(columns)
              .mapToLong(InsertNodeMemoryEstimator::getNumBytesUnknownObject)
              .reduce(0L, Long::sum);
    }
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * columns.length);
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] == null
          || i >= measurementSchemas.length
          || measurementSchemas[i] == null
          || measurementSchemas[i].getType() == null) {
        continue;
      }
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
        case OBJECT:
          {
            size += RamUsageEstimator.sizeOf((Binary[]) columns[i]);
            break;
          }
      }
    }
    return size;
  }

  private static long getNumBytesUnknownObject(final Object obj) {
    return obj instanceof Binary[]
        ? RamUsageEstimator.sizeOf((Binary[]) obj)
        : RamUsageEstimator.sizeOfObject(obj);
  }

  public static long sizeOfValues(
      final Object[] values, final MeasurementSchema[] measurementSchemas) {
    if (Objects.isNull(values)) {
      return 0L;
    }
    // Directly calculate if measurementSchemas are absent
    if (Objects.isNull(measurementSchemas)) {
      return RamUsageEstimator.shallowSizeOf(values)
          + Arrays.stream(values)
              .mapToLong(InsertNodeMemoryEstimator::getNumBytesUnknownObject)
              .reduce(0L, Long::sum);
    }
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * values.length);
    for (int i = 0; i < values.length; i++) {
      if (i >= measurementSchemas.length
          || measurementSchemas[i] == null
          || measurementSchemas[i].getType() == null) {
        size += NUM_BYTES_OBJECT_HEADER;
        continue;
      }
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
            final Binary binary = (Binary) values[i];
            size += sizeOfBinary(binary);
          }
      }
    }
    return size;
  }

  private static long sizeOfPlanNodeId(
      final PlanNodeId planNodeId, final Set<Object> deduplicatedObjects) {
    return planNodeId != null && shouldCountObject(planNodeId, deduplicatedObjects)
        ? planNodeId.ramBytesUsed()
        : 0L;
  }

  private static long sizeOfStringArray(
      final String[] strings, final Set<Object> deduplicatedObjects) {
    return strings != null && shouldCountObject(strings, deduplicatedObjects)
        ? RamUsageEstimator.sizeOf(strings)
        : 0L;
  }

  private static long sizeOfShallowObject(
      final Object object, final Set<Object> deduplicatedObjects) {
    return object != null && shouldCountObject(object, deduplicatedObjects)
        ? RamUsageEstimator.shallowSizeOf(object)
        : 0L;
  }

  private static long sizeOfInsertRowNodeList(
      final List<InsertRowNode> rows, final Set<Object> deduplicatedObjects) {
    if (rows == null) {
      return 0L;
    }
    long size = sizeOfObjectList(rows);
    for (InsertRowNode row : rows) {
      size += sizeOfContainedInsertRowNode(row, deduplicatedObjects);
    }
    return size;
  }

  private static long sizeOfInsertTabletNodeList(
      final List<InsertTabletNode> tablets, final Set<Object> deduplicatedObjects) {
    if (tablets == null) {
      return 0L;
    }
    long size = sizeOfObjectList(tablets);
    for (InsertTabletNode tablet : tablets) {
      size += sizeOfInsertTabletNode(tablet, deduplicatedObjects);
    }
    return size;
  }

  private static long sizeOfContainedInsertRowNode(
      final InsertRowNode node, final Set<Object> deduplicatedObjects) {
    return node instanceof RelationalInsertRowNode
        ? sizeOfRelationalInsertRowNode((RelationalInsertRowNode) node, deduplicatedObjects)
        : sizeOfInsertRowNode(node, deduplicatedObjects);
  }

  private static long sizeOfObjectList(final List<?> list) {
    if (list == null) {
      return 0L;
    }
    long size = RamUsageEstimator.shallowSizeOf(list);
    if (list instanceof ArrayList) {
      size +=
          RamUsageEstimator.alignObjectSize(
              NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * list.size());
    }
    return size;
  }

  private static long sizeOfIntegerList(final List<Integer> integers) {
    if (integers == null) {
      return 0L;
    }
    long size = sizeOfObjectList(integers);
    for (Integer ignored : integers) {
      size += SIZE_OF_INT;
    }
    return size;
  }

  private static long sizeOfResults(final Map<Integer, TSStatus> results) {
    if (results == null) {
      return 0L;
    }
    long size = RamUsageEstimator.shallowSizeOf(results);
    for (Map.Entry<Integer, TSStatus> entry : results.entrySet()) {
      size +=
          SIZE_OF_INT
              + sizeOfTSStatus(entry.getValue())
              + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
    }
    return size;
  }

  private static Set<Object> newDeduplicatedObjectSet() {
    return Collections.newSetFromMap(new IdentityHashMap<>());
  }

  private static boolean shouldCountObject(
      final Object object, final Set<Object> deduplicatedObjects) {
    return object != null && (deduplicatedObjects == null || deduplicatedObjects.add(object));
  }
}
