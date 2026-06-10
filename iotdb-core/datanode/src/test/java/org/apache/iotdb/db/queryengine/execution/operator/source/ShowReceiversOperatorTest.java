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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeRegistry;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.CONNECTION_COUNT;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LAST_HANDSHAKE_TIME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LAST_TRANSFER_TIME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PIPE_COUNT;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PIPE_IDS;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PROTOCOL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.RECEIVER_NODE_ID;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.RECEIVER_NODE_TYPE;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.RECEIVER_USER_NAME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_ADDRESS;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_CLUSTER_ID;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_PORTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShowReceiversOperatorTest {

  private final PipeReceiverRuntimeRegistry registry = PipeReceiverRuntimeRegistry.getInstance();

  @Before
  public void setUp() {
    registry.clear();
  }

  @After
  public void tearDown() {
    registry.clear();
  }

  @Test
  public void testShowReceiversHeaderColumns() {
    assertEquals(
        ImmutableList.of(
            RECEIVER_NODE_TYPE,
            RECEIVER_NODE_ID,
            PROTOCOL,
            SENDER_ADDRESS,
            SENDER_PORTS,
            CONNECTION_COUNT,
            PIPE_COUNT,
            PIPE_IDS,
            RECEIVER_USER_NAME,
            SENDER_CLUSTER_ID,
            LAST_HANDSHAKE_TIME,
            LAST_TRANSFER_TIME),
        DatasetHeaderFactory.getShowReceiversHeader().getRespColumns());
  }

  @Test
  public void testUnknownReceiverNodeIdIsNull() {
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        -1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(null, new PlanNodeId("show-receivers"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals(12, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(1).isNull(0));
  }
}
