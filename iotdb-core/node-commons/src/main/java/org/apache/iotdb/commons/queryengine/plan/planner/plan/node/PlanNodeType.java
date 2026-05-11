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

package org.apache.iotdb.commons.queryengine.plan.planner.plan.node;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ServiceLoader;

public enum PlanNodeType {
  AGGREGATE((short) 0),
  DEVICE_VIEW((short) 1),
  FILL((short) 2),
  FILTER((short) 3),
  FILTER_NULL((short) 4),
  GROUP_BY_LEVEL((short) 5),
  LIMIT((short) 6),
  OFFSET((short) 7),
  SORT((short) 8),
  FULL_OUTER_TIME_JOIN((short) 9),
  FRAGMENT_SINK((short) 10),
  SERIES_SCAN((short) 11),
  SERIES_AGGREGATE_SCAN((short) 12),
  INSERT_TABLET((short) 13),
  INSERT_ROW((short) 14),
  INSERT_ROWS((short) 15),
  INSERT_ROWS_OF_ONE_DEVICE((short) 16),
  INSERT_MULTI_TABLET((short) 17),
  DEVICES_SCHEMA_SCAN((short) 18),
  CREATE_TIME_SERIES((short) 19),
  EXCHANGE((short) 20),
  ALTER_TIME_SERIES((short) 21),
  CREATE_ALIGNED_TIME_SERIES((short) 22),
  TIME_SERIES_SCHEMA_SCAN((short) 23),
  SERIES_SCHEMA_FETCH_SCAN((short) 24),
  SCHEMA_QUERY_MERGE((short) 25),
  SCHEMA_QUERY_ORDER_BY_HEAT((short) 26),
  DEVICES_COUNT((short) 27),
  TIME_SERIES_COUNT((short) 28),
  LEVEL_TIME_SERIES_COUNT((short) 29),
  COUNT_MERGE((short) 30),
  SLIDING_WINDOW_AGGREGATION((short) 31),
  PROJECT((short) 32),
  ALIGNED_SERIES_SCAN((short) 33),
  ALIGNED_SERIES_AGGREGATE_SCAN((short) 34),
  DEVICE_MERGE((short) 35),
  SCHEMA_FETCH_MERGE((short) 36),
  TRANSFORM((short) 37),
  CREATE_MULTI_TIME_SERIES((short) 39),
  NODE_PATHS_SCAN((short) 40),
  NODE_PATHS_CONVERT((short) 41),
  NODE_MANAGEMENT_MEMORY_MERGE((short) 42),
  DELETE_DATA((short) 44),
  DELETE_TIME_SERIES((short) 45),
  @Deprecated
  DEPRECATED_LAST_QUERY_SCAN((short) 46),
  @Deprecated
  DEPRECATED_ALIGNED_LAST_QUERY_SCAN((short) 47),
  LAST_QUERY((short) 48),
  LAST_QUERY_MERGE((short) 49),
  LAST_QUERY_COLLECT((short) 50),
  NODE_PATHS_COUNT((short) 51),
  INTERNAL_CREATE_TIME_SERIES((short) 52),
  ACTIVATE_TEMPLATE((short) 53),
  PATHS_USING_TEMPLATE_SCAN((short) 54),
  LOAD_TSFILE((short) 55),
  CONSTRUCT_SCHEMA_BLACK_LIST_NODE((short) 56),
  ROLLBACK_SCHEMA_BLACK_LIST_NODE((short) 57),
  GROUP_BY_TAG((short) 58),
  PRE_DEACTIVATE_TEMPLATE_NODE((short) 59),
  ROLLBACK_PRE_DEACTIVATE_TEMPLATE_NODE((short) 60),
  DEACTIVATE_TEMPLATE_NODE((short) 61),
  INTO((short) 62),
  DEVICE_VIEW_INTO((short) 63),
  VERTICALLY_CONCAT((short) 64),
  SINGLE_DEVICE_VIEW((short) 65),
  MERGE_SORT((short) 66),
  SHOW_QUERIES((short) 67),
  INTERNAL_BATCH_ACTIVATE_TEMPLATE((short) 68),
  INTERNAL_CREATE_MULTI_TIMESERIES((short) 69),
  IDENTITY_SINK((short) 70),
  SHUFFLE_SINK((short) 71),
  BATCH_ACTIVATE_TEMPLATE((short) 72),
  CREATE_LOGICAL_VIEW((short) 73),
  CONSTRUCT_LOGICAL_VIEW_BLACK_LIST((short) 74),
  ROLLBACK_LOGICAL_VIEW_BLACK_LIST((short) 75),
  DELETE_LOGICAL_VIEW((short) 76),
  LOGICAL_VIEW_SCHEMA_SCAN((short) 77),
  ALTER_LOGICAL_VIEW((short) 78),
  PIPE_ENRICHED_INSERT_DATA((short) 79),
  INFERENCE((short) 80),
  LAST_QUERY_TRANSFORM((short) 81),
  TOP_K((short) 82),
  COLUMN_INJECT((short) 83),

  PIPE_ENRICHED_DELETE_DATA((short) 84),
  PIPE_ENRICHED_WRITE((short) 85),
  PIPE_ENRICHED_NON_WRITE((short) 86),

  INNER_TIME_JOIN((short) 87),
  LEFT_OUTER_TIME_JOIN((short) 88),
  AGG_MERGE_SORT((short) 89),

  EXPLAIN_ANALYZE((short) 90),

  PIPE_OPERATE_SCHEMA_QUEUE_REFERENCE((short) 91),

  RAW_DATA_AGGREGATION((short) 92),

  DEVICE_REGION_SCAN((short) 93),
  TIMESERIES_REGION_SCAN((short) 94),
  REGION_MERGE((short) 95),
  DEVICE_SCHEMA_FETCH_SCAN((short) 96),

  CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR((short) 97),

  LAST_QUERY_SCAN((short) 98),
  ALTER_ENCODING_COMPRESSOR((short) 99),
  // 100 - 106 are occupied
  SHOW_DISK_USAGE((short) 107),
  TREE_COLLECT((short) 108),
  LOAD_TSFILE_OBJECT_PIECE((short) 109),

  CREATE_OR_UPDATE_TABLE_DEVICE((short) 902),
  TABLE_DEVICE_QUERY_SCAN((short) 903),
  TABLE_DEVICE_FETCH((short) 904),
  DELETE_TABLE_DEVICE((short) 905),
  TABLE_DEVICE_QUERY_COUNT((short) 906),
  TABLE_DEVICE_ATTRIBUTE_UPDATE((short) 907),
  TABLE_DEVICE_ATTRIBUTE_COMMIT((short) 908),
  TABLE_DEVICE_LOCATION_ADD((short) 909),
  CONSTRUCT_TABLE_DEVICES_BLACK_LIST((short) 910),
  ROLLBACK_TABLE_DEVICES_BLACK_LIST((short) 911),
  DELETE_TABLE_DEVICES_IN_BLACK_LIST((short) 912),
  TABLE_ATTRIBUTE_COLUMN_DROP((short) 913),

  DEVICE_TABLE_SCAN_NODE((short) 1000),
  TABLE_FILTER_NODE((short) 1001),
  TABLE_PROJECT_NODE((short) 1002),
  TABLE_OUTPUT_NODE((short) 1003),
  TABLE_LIMIT_NODE((short) 1004),
  TABLE_OFFSET_NODE((short) 1005),
  TABLE_SORT_NODE((short) 1006),
  TABLE_MERGESORT_NODE((short) 1007),
  TABLE_TOPK_NODE((short) 1008),
  TABLE_COLLECT_NODE((short) 1009),
  TABLE_STREAM_SORT_NODE((short) 1010),
  TABLE_JOIN_NODE((short) 1011),
  TABLE_PREVIOUS_FILL_NODE((short) 1012),
  TABLE_LINEAR_FILL_NODE((short) 1013),
  TABLE_VALUE_FILL_NODE((short) 1014),
  TABLE_AGGREGATION_NODE((short) 1015),
  TABLE_AGGREGATION_TABLE_SCAN_NODE((short) 1016),
  TABLE_GAP_FILL_NODE((short) 1017),
  TABLE_EXCHANGE_NODE((short) 1018),
  TABLE_EXPLAIN_ANALYZE_NODE((short) 1019),
  TABLE_ENFORCE_SINGLE_ROW_NODE((short) 1020),
  INFORMATION_SCHEMA_TABLE_SCAN_NODE((short) 1021),
  @Deprecated
  AGGREGATION_TREE_DEVICE_VIEW_SCAN_NODE((short) 1022),
  TREE_ALIGNED_DEVICE_VIEW_SCAN_NODE((short) 1023),
  TREE_NONALIGNED_DEVICE_VIEW_SCAN_NODE((short) 1024),
  TABLE_SEMI_JOIN_NODE((short) 1025),
  MARK_DISTINCT_NODE((short) 1026),
  TABLE_ASSIGN_UNIQUE_ID((short) 1027),
  TABLE_FUNCTION_NODE((short) 1028),
  TABLE_FUNCTION_PROCESSOR_NODE((short) 1029),
  TABLE_GROUP_NODE((short) 1030),
  TABLE_PATTERN_RECOGNITION_NODE((short) 1031),
  TABLE_WINDOW_FUNCTION((short) 1032),
  TABLE_INTO_NODE((short) 1033),
  TABLE_UNION_NODE((short) 1034),
  TABLE_INTERSECT_NODE((short) 1035),
  TABLE_EXCEPT_NODE((short) 1036),
  TABLE_TOPK_RANKING_NODE((short) 1037),
  TABLE_ROW_NUMBER_NODE((short) 1038),
  TABLE_VALUES_NODE((short) 1039),
  TABLE_DISK_USAGE_INFORMATION_SCHEMA_TABLE_SCAN_NODE((short) 1040),
  ALIGNED_AGGREGATION_TREE_DEVICE_VIEW_SCAN_NODE((short) 1041),
  NON_ALIGNED_AGGREGATION_TREE_DEVICE_VIEW_SCAN_NODE((short) 1042),

  RELATIONAL_INSERT_TABLET((short) 2000),
  RELATIONAL_INSERT_ROW((short) 2001),
  RELATIONAL_INSERT_ROWS((short) 2002),
  RELATIONAL_DELETE_DATA((short) 2003),
  OBJECT_FILE_NODE((short) 2004),
  ;

  private static final IPlanNodeDeserializer DESERIALIZER;

  static {
    IPlanNodeDeserializer deserializer = null;
    ServiceLoader<IPlanNodeDeserializerProvider> loader =
        ServiceLoader.load(IPlanNodeDeserializerProvider.class);
    for (IPlanNodeDeserializerProvider provider : loader) {
      if (deserializer != null) {
        throw new IllegalStateException("Multiple IPlanNodeDeserializerProvider found");
      }
      deserializer = provider.getDeserializer();
    }
    DESERIALIZER = deserializer == null ? CommonPlanNodeDeserializer.INSTANCE : deserializer;
  }

  public static final int BYTES = Short.BYTES;

  private final short nodeType;

  PlanNodeType(short nodeType) {
    this.nodeType = nodeType;
  }

  public short getNodeType() {
    return nodeType;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(nodeType, buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(nodeType, stream);
  }

  public static PlanNode deserializeFromWAL(DataInputStream stream) throws IOException {
    return DESERIALIZER.deserializeFromWAL(stream);
  }

  public static PlanNode deserializeFromWAL(ByteBuffer buffer) {
    return DESERIALIZER.deserializeFromWAL(buffer);
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    return DESERIALIZER.deserialize(buffer);
  }

  public static PlanNode deserialize(ByteBuffer buffer, short nodeType) {
    return DESERIALIZER.deserialize(buffer, nodeType);
  }
}
