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
package org.apache.iotdb.db.mpp.sql.planner.plan.node;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaFetchNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.ShowDevicesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AuthorNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertMultiTabletsNode;

import java.nio.ByteBuffer;

public enum PlanNodeType {
  AGGREGATE((short) 0),
  DEVICE_MERGE((short) 1),
  FILL((short) 2),
  FILTER((short) 3),
  FILTER_NULL((short) 4),
  GROUP_BY_LEVEL((short) 5),
  LIMIT((short) 6),
  OFFSET((short) 7),
  SORT((short) 8),
  TIME_JOIN((short) 9),
  FRAGMENT_SINK((short) 10),
  SERIES_SCAN((short) 11),
  SERIES_AGGREGATE_SCAN((short) 12),
  INSERT_TABLET((short) 13),
  INSERT_ROW((short) 14),
  INSERT_ROWS((short) 15),
  INSERT_ROWS_OF_ONE_DEVICE((short) 16),
  INSERT_MULTI_TABLET((short) 17),
  SHOW_DEVICES((short) 18),
  CREATE_TIME_SERIES((short) 19),
  EXCHANGE((short) 20),
  AUTHOR((short) 21),
  ALTER_TIME_SERIES((short) 22),
  CREATE_ALIGNED_TIME_SERIES((short) 23),
  SCHEMA_FETCH((short) 24);

  private final short nodeType;

  PlanNodeType(short nodeType) {
    this.nodeType = nodeType;
  }

  public void serialize(ByteBuffer buffer) {
    buffer.putShort(nodeType);
  }

  public static PlanNode deserialize(ByteBuffer buffer) throws IllegalPathException {
    short nodeType = buffer.getShort();
    switch (nodeType) {
      case 0:
        return AggregateNode.deserialize(buffer);
      case 1:
        return DeviceMergeNode.deserialize(buffer);
      case 2:
        return FillNode.deserialize(buffer);
      case 3:
        return FilterNode.deserialize(buffer);
      case 4:
        return FilterNullNode.deserialize(buffer);
      case 5:
        return GroupByLevelNode.deserialize(buffer);
      case 6:
        return LimitNode.deserialize(buffer);
      case 7:
        return OffsetNode.deserialize(buffer);
      case 8:
        return SortNode.deserialize(buffer);
      case 9:
        return TimeJoinNode.deserialize(buffer);
      case 10:
        return FragmentSinkNode.deserialize(buffer);
      case 11:
        return SeriesScanNode.deserialize(buffer);
      case 12:
        return SeriesAggregateScanNode.deserialize(buffer);
      case 13:
        return InsertTabletNode.deserialize(buffer);
      case 14:
        return InsertRowNode.deserialize(buffer);
      case 15:
        return InsertRowsNode.deserialize(buffer);
      case 16:
        return InsertRowsOfOneDeviceNode.deserialize(buffer);
      case 17:
        return InsertMultiTabletsNode.deserialize(buffer);
      case 18:
        return ShowDevicesNode.deserialize(buffer);
      case 19:
        return CreateTimeSeriesNode.deserialize(buffer);
      case 20:
        return ExchangeNode.deserialize(buffer);
      case 21:
        return AuthorNode.deserialize(buffer);
      case 22:
        return AlterTimeSeriesNode.deserialize(buffer);
      case 23:
        return CreateAlignedTimeSeriesNode.deserialize(buffer);
      case 24:
        return SchemaFetchNode.deserialize(buffer);
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
  }
}
