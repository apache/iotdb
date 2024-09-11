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
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InsertNodeMemoryEstimator {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertNodeMemoryEstimator.class);

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

  private static final long TS_ENCODING_PLAIN_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.PLAIN));

  private static final long TS_ENCODING_DICTIONARY_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.DICTIONARY));

  private static final long TS_ENCODING_RLE_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.RLE));

  private static final long TS_ENCODING_TS_2DIFF_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.TS_2DIFF));

  private static final long TS_ENCODING_GORILLA_V1_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.GORILLA_V1));

  private static final long TS_ENCODING_REGULAR_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.REGULAR));

  private static final long TS_ENCODING_GORILLA_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.GORILLA));

  private static final long TS_ENCODING_ZIGZAG_BUILDER_SIZE =
      RamUsageEstimator.shallowSizeOf(TSEncodingBuilder.getEncodingBuilder(TSEncoding.ZIGZAG));

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

  private static final long INSERT_TABLET_NODE_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + INSERT_NODE_PRIMITIVE_SIZE
              + INSERT_NODE_REFERENCE_SIZE
              + INSERT_TABLET_NODE_PRIMITIVE_SIZE
              + INSERT_TABLET_NODE_REFERENCE_SIZE);

  private static final long INSERT_ROW_NODE_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + INSERT_NODE_PRIMITIVE_SIZE
              + INSERT_NODE_REFERENCE_SIZE
              + INSERT_ROW_NODE_PRIMITIVE_SIZE
              + INSERT_ROW_NODE_REFERENCE_SIZE);

  private static final long INSERT_ROWS_NODE_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + INSERT_NODE_PRIMITIVE_SIZE
              + INSERT_NODE_REFERENCE_SIZE
              + INSERT_ROWS_NODE_REFERENCE_SIZE);

  private static final long INSERT_ROWS_OF_ONE_DEVICE_NODE_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + INSERT_NODE_PRIMITIVE_SIZE
              + INSERT_NODE_REFERENCE_SIZE
              + INSERT_ROWS_OF_ONE_DEVICE_NODE_REFERENCE_SIZE);

  private static final long INSERT_MULTI_TABLETS_NODE_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + INSERT_NODE_PRIMITIVE_SIZE
              + INSERT_NODE_REFERENCE_SIZE
              + INSERT_ROWS_OF_ONE_DEVICE_NODE_REFERENCE_SIZE);

  private static final long RELATIONAL_INSERT_ROWS_NODE_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + INSERT_NODE_PRIMITIVE_SIZE
              + INSERT_NODE_REFERENCE_SIZE
              + RELATIONAL_INSERT_ROWS_NODE_REFERENCE_SIZE);

  private static final long RELATIONAL_INSERT_ROW_NODE_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + INSERT_NODE_PRIMITIVE_SIZE
              + INSERT_NODE_REFERENCE_SIZE
              + RELATIONAL_INSERT_ROW_NODE_PRIMITIVE_SIZE
              + RELATIONAL_INSERT_ROW_NODE_REFERENCE_SIZE);

  private static final long RELATIONAL_INSERT_TABLET_NODE_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + INSERT_NODE_PRIMITIVE_SIZE
              + INSERT_NODE_REFERENCE_SIZE
              + RELATIONAL_INSERT_TABLET_NODE_PRIMITIVE_SIZE
              + RELATIONAL_INSERT_TABLET_NODE_REFERENCE_SIZE);

  // ============================Device And Measurement===================================

  // Calculate the total size of reference types in PartialPath
  private static final long PARTIAL_PATH_REFERENCE_SIZE =
      4L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long PARTIAL_PATH_SIZE =
      RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + PARTIAL_PATH_REFERENCE_SIZE);

  // Calculate the total size of primitive fields in Measurement
  private static final long MEASUREMENT_PRIMITIVE_SIZE = 3L; // type + encoding +  compressor

  // Calculate the total size of reference types in Measurement
  private static final long MEASUREMENT_REFERENCE_SIZE =
      3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long MEASUREMENT_SCHEMA_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + MEASUREMENT_PRIMITIVE_SIZE + MEASUREMENT_REFERENCE_SIZE);

  // Calculate the total size of reference types in PlainDeviceID
  private static final long PLAIN_DEVICE_ID_REFERENCE_SIZE =
      3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long PLAIN_DEVICE_ID_SIZE =
      RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + PLAIN_DEVICE_ID_REFERENCE_SIZE);

  // =============================Thrift==================================

  // Calculate the total size of reference types in TRegionReplicaSet
  private static final long T_REGION_REPLICA_SET_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long T_REGION_REPLICA_SET_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + T_REGION_REPLICA_SET_REFERENCE_SIZE);

  // Calculate the total size of reference types in TDataNodeLocation
  private static final long T_DATA_NODE_LOCATION_REFERENCE_SIZE =
      5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long T_DATA_NODE_LOCATION_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + T_DATA_NODE_LOCATION_REFERENCE_SIZE + Integer.BYTES);

  // Calculate the total size of reference types in TSStatus
  private static final long TS_STATUS_REFERENCE_SIZE = 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in TSStatus
  private static final long TS_STATUS_PRIMITIVE_SIZE = Integer.BYTES + 1;

  private static final long TS_STATUS_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + TS_STATUS_PRIMITIVE_SIZE + TS_STATUS_REFERENCE_SIZE);

  private static final long T_END_POINT_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF + Integer.BYTES);

  private static final long T_CONSENSUS_GROUP_ID_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF + Integer.BYTES);

  // =============================ProgressIndex==================================

  // Calculate the total size of reference types in HybridProgressIndex
  private static final long HYBRID_PROGRESS_INDEX_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long HYBRID_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + HYBRID_PROGRESS_INDEX_REFERENCE_SIZE);

  // Calculate the total size of reference types in IoTProgressIndex
  private static final long IO_T_PROGRESS_INDEX_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long IO_T_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + IO_T_PROGRESS_INDEX_REFERENCE_SIZE);

  // Calculate the total size of reference types in MetaProgressIndex
  private static final long META_PROGRESS_INDEX_REFERENCE_SIZE =
      RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in MetaProgressIndex
  private static final long META_PROGRESS_INDEX_PRIMITIVE_SIZE = Long.BYTES;

  private static final long META_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + META_PROGRESS_INDEX_PRIMITIVE_SIZE
              + META_PROGRESS_INDEX_REFERENCE_SIZE);

  // Calculate the total size of reference types in RecoverProgressIndex
  private static final long RECOVER_PROGRESS_INDEX_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long RECOVER_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + RECOVER_PROGRESS_INDEX_REFERENCE_SIZE);

  // Calculate the total size of reference types in SimpleProgressIndex
  private static final long SIMPLE_PROGRESS_INDEX_REFERENCE_SIZE =
      RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in SimpleProgressIndex
  private static final long SIMPLE_PROGRESS_INDEX_PRIMITIVE_SIZE = Integer.BYTES + Long.BYTES;

  private static final long SIMPLE_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + SIMPLE_PROGRESS_INDEX_PRIMITIVE_SIZE
              + SIMPLE_PROGRESS_INDEX_REFERENCE_SIZE);

  // Calculate the total size of reference types in StateProgressIndex
  private static final long STATE_PROGRESS_INDEX_REFERENCE_SIZE =
      3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in StateProgressIndex
  private static final long STATE_PROGRESS_INDEX_PRIMITIVE_SIZE = Long.BYTES;

  private static final long STATE_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER
              + STATE_PROGRESS_INDEX_PRIMITIVE_SIZE
              + STATE_PROGRESS_INDEX_REFERENCE_SIZE);

  // Calculate the total size of reference types in TimeWindowStateProgressIndex
  private static final long TIME_WINDOW_STATE_PROGRESS_INDEX_REFERENCE_SIZE =
      2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private static final long TIME_WINDOW_STATE_PROGRESS_INDEX_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + TIME_WINDOW_STATE_PROGRESS_INDEX_REFERENCE_SIZE);

  // =============================BitMap==================================
  // Calculate the total size of reference types in BitMap
  private static final long BIT_MAP_REFERENCE_SIZE = RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  // Calculate the total size of primitive fields in BitMap
  private static final long BIT_MAP_PRIMITIVE_SIZE = Integer.BYTES;

  private static final long BIT_MAP_SIZE =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + BIT_MAP_REFERENCE_SIZE + BIT_MAP_PRIMITIVE_SIZE);

  // ============================= Primitive Type Wrapper Classes =========

  public static final long SIZE_OF_LONG =
      RamUsageEstimator.alignObjectSize(Long.BYTES + NUM_BYTES_OBJECT_HEADER);
  public static final long SIZE_OF_INT =
      RamUsageEstimator.alignObjectSize(Integer.BYTES + NUM_BYTES_OBJECT_HEADER);
  public static final long SIZE_OF_DOUBLE =
      RamUsageEstimator.alignObjectSize(Double.BYTES + NUM_BYTES_OBJECT_HEADER);
  public static final long SIZE_OF_FLOAT =
      RamUsageEstimator.alignObjectSize(Float.BYTES + NUM_BYTES_OBJECT_HEADER);
  public static final long SIZE_OF_BOOLEAN =
      RamUsageEstimator.alignObjectSize(1 + NUM_BYTES_OBJECT_HEADER);

  // The calculated result needs to be magnified by 1.3 times, which is 1.3 times different
  // from the actual result because the properties of the parent class are not added.
  private static final double INSERT_ROW_NODE_EXPANSION_FACTOR = 1.3;

  public static long sizeOf(final InsertNode insertNode) {
    try {
      final String className = insertNode.getClass().getName();
      switch (className) {
        case INSERT_TABLET_NODE_CLASS_NAME:
          return sizeOfInsertTabletNode((InsertTabletNode) insertNode);
        case INSERT_ROW_NODE_CLASS_NAME:
          return (long)
              (sizeOfInsertRowNode((InsertRowNode) insertNode) * INSERT_ROW_NODE_EXPANSION_FACTOR);
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
    } catch (Exception e) {
      LOGGER.warn("Failed to estimate size for InsertNode: {}", e.getMessage(), e);
      return 0L;
    }
  }

  // =============================InsertNode==================================

  private static long calculateFullInsertNodeSize(final InsertNode node) {
    long size = 0;
    // PartialPath
    size += sizeOfPartialPath(node.getDevicePath());
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
    size += sizeOfIDeviceID(node.getDeviceID());
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
    size += sizeOfIDeviceID(node.getDeviceID());
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
      size += NUM_BYTES_OBJECT_HEADER + (Integer.BYTES + NUM_BYTES_OBJECT_REF) * range.size();
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
      size += NUM_BYTES_OBJECT_HEADER + (NUM_BYTES_OBJECT_REF * Integer.BYTES) * range.size();
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

    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      // InsertRowNodeList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (calculateInsertRowNodeExcludingSchemas(rows.get(0)) + NUM_BYTES_OBJECT_REF)
              * rows.size();
      size += sizeOfPartialPath(rows.get(0).getDevicePath());
      size += sizeOfMeasurementSchemas(rows.get(0).getMeasurementSchemas());
      // InsertRowNodeIndexList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (long) indexList.size()
              * (Integer.BYTES + Integer.BYTES + NUM_BYTES_OBJECT_REF + NUM_BYTES_OBJECT_HEADER);
    }
    return size;
  }

  private static long sizeOfInsertRowsOfOneDeviceNode(final InsertRowsOfOneDeviceNode node) {
    long size = INSERT_ROWS_OF_ONE_DEVICE_NODE_SIZE;

    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      // InsertRowNodeList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (calculateInsertRowNodeExcludingSchemas(rows.get(0)) + NUM_BYTES_OBJECT_REF)
              * rows.size();
      size += sizeOfPartialPath(rows.get(0).getDevicePath());
      size += sizeOfMeasurementSchemas(rows.get(0).getMeasurementSchemas());
      // InsertRowNodeIndexList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (long) indexList.size()
              * (Integer.BYTES + Integer.BYTES + NUM_BYTES_OBJECT_REF + NUM_BYTES_OBJECT_HEADER);
    }
    // results
    size += NUM_BYTES_OBJECT_HEADER;
    for (Map.Entry<Integer, TSStatus> entry : node.getResults().entrySet()) {
      size += Integer.BYTES + sizeOfTSStatus(entry.getValue());
    }
    return size;
  }

  private static long sizeOfInsertMultiTabletsNode(final InsertMultiTabletsNode node) {
    long size = INSERT_MULTI_TABLETS_NODE_SIZE;
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
      size += sizeOfPartialPath(rows.get(0).getDevicePath());
      size += sizeOfMeasurementSchemas(rows.get(0).getMeasurementSchemas());
      // ParentInsertTabletNodeIndexList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (long) indexList.size()
              * (Integer.BYTES + Integer.BYTES + NUM_BYTES_OBJECT_REF + NUM_BYTES_OBJECT_HEADER);
    }
    // results
    size += NUM_BYTES_OBJECT_HEADER;
    for (Map.Entry<Integer, TSStatus> entry : node.getResults().entrySet()) {
      size += Integer.BYTES + sizeOfTSStatus(entry.getValue());
    }
    return size;
  }

  private static long sizeOfRelationalInsertRowsNode(final RelationalInsertRowsNode node) {
    long size = RELATIONAL_INSERT_ROWS_NODE_SIZE;

    final List<InsertRowNode> rows = node.getInsertRowNodeList();
    final List<Integer> indexList = node.getInsertRowNodeIndexList();
    if (rows != null && !rows.isEmpty()) {
      // InsertRowNodeList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (calculateInsertRowNodeExcludingSchemas(rows.get(0)) + NUM_BYTES_OBJECT_REF)
              * rows.size();
      size += sizeOfPartialPath(rows.get(0).getDevicePath());
      size += sizeOfMeasurementSchemas(rows.get(0).getMeasurementSchemas());
      // InsertRowNodeIndexList
      size += NUM_BYTES_OBJECT_HEADER;
      size +=
          (long) indexList.size()
              * (Integer.BYTES + Integer.BYTES + NUM_BYTES_OBJECT_REF + NUM_BYTES_OBJECT_HEADER);
    }
    // ignore deviceIDs
    return size;
  }

  private static long sizeOfRelationalInsertRowNode(final RelationalInsertRowNode node) {
    long size = RELATIONAL_INSERT_ROW_NODE_SIZE;

    size += sizeOfValues(node.getValues(), node.getMeasurementSchemas());
    size += sizeOfIDeviceID(node.getDeviceID());
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
        size += sizeOfString(entry.getKey()) + sizeOfString(entry.getValue());
      }
    }
    // load EncodingBuilder
    switch (measurementSchema.getValueEncoder().getType()) {
      case PLAIN:
        size += TS_ENCODING_PLAIN_BUILDER_SIZE;
        break;
      case DICTIONARY:
        size += TS_ENCODING_DICTIONARY_BUILDER_SIZE;
        break;
      case RLE:
        size += TS_ENCODING_RLE_BUILDER_SIZE;
        break;
      case TS_2DIFF:
        size += TS_ENCODING_TS_2DIFF_BUILDER_SIZE;
        break;
      case GORILLA_V1:
        size += TS_ENCODING_GORILLA_V1_BUILDER_SIZE;
        break;
      case REGULAR:
        size += TS_ENCODING_REGULAR_BUILDER_SIZE;
        break;
      case GORILLA:
        size += TS_ENCODING_GORILLA_BUILDER_SIZE;
        break;
      case ZIGZAG:
        size += TS_ENCODING_ZIGZAG_BUILDER_SIZE;
    }

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
    long size =
        RamUsageEstimator.alignObjectSize(
            NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF * columnCategories.length);
    for (TsTableColumnCategory columnCategory : columnCategories) {
      if (columnCategory != null && columnCategory.equals(TsTableColumnCategory.ID)) {
        size += Integer.BYTES + NUM_BYTES_OBJECT_REF;
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
      size += sizeOfString(id);
      // table
      size += sizeOfString(deviceID.getTableName());
      // segments[]
      int num = deviceID.segmentNum();
      size +=
          RamUsageEstimator.alignObjectSize(
              NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * deviceID.segmentNum());
      while (num > 0) {
        size += sizeOfString(deviceID.segment(--num));
      }
    }

    return size;
  }

  private static long sizeOfStringArrayDeviceID(final StringArrayDeviceID deviceID) {
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
        return sizeOfMinimumProgressIndex();
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
      size +=
          NUM_BYTES_OBJECT_HEADER
              + progressIndex.getType2Index().size() * (Short.BYTES + NUM_BYTES_OBJECT_REF);
    }
    return size;
  }

  private static long sizeOfIoTProgressIndex(IoTProgressIndex progressIndex) {
    // Memory alignment of basic types and reference types in structures
    long size = IO_T_PROGRESS_INDEX_SIZE;

    // Memory calculation in reference type, cannot get exact value, roughly estimate
    size += REENTRANT_READ_WRITE_LOCK_SIZE;

    size +=
        NUM_BYTES_OBJECT_HEADER
            + progressIndex.getPeerId2SearchIndexSize() * (long) (Integer.BYTES + Long.BYTES);

    return size;
  }

  private static long sizeOfMetaProgressIndex() {
    // Memory alignment of basic types and reference types in structures
    return META_PROGRESS_INDEX_SIZE + REENTRANT_READ_WRITE_LOCK_SIZE;
  }

  private static long sizeOfMinimumProgressIndex() {
    // Memory alignment of basic types and reference types in structures
    return RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER);
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
                  * (long) (Integer.BYTES + Long.BYTES);
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
        size += sizeOfString(entry.getKey()) + sizeOfBinary(entry.getValue());
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
        size += sizeOfString(entry.getKey()) + Long.BYTES * 2;
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
    return RamUsageEstimator.alignObjectSize(NUM_BYTES_OBJECT_HEADER + NUM_BYTES_OBJECT_REF)
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
