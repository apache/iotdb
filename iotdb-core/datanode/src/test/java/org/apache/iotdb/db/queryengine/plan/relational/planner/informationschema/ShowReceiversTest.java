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

package org.apache.iotdb.db.queryengine.plan.relational.planner.informationschema;

import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Optional;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.CONNECTION_COUNT_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LAST_HANDSHAKE_TIME_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LAST_TRANSFER_TIME_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PIPE_COUNT_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PIPE_IDS_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PROTOCOL_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.RECEIVER_NODE_ID_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.RECEIVER_NODE_TYPE_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_ADDRESS_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_CLUSTER_ID_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_PORTS_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.USER_NAME;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.infoSchemaTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;

public class ShowReceiversTest {

  private static final ImmutableList<String> RECEIVERS_COLUMNS =
      ImmutableList.of(
          RECEIVER_NODE_TYPE_TABLE_MODEL,
          RECEIVER_NODE_ID_TABLE_MODEL,
          PROTOCOL_TABLE_MODEL,
          SENDER_ADDRESS_TABLE_MODEL,
          SENDER_PORTS_TABLE_MODEL,
          CONNECTION_COUNT_TABLE_MODEL,
          PIPE_COUNT_TABLE_MODEL,
          PIPE_IDS_TABLE_MODEL,
          USER_NAME,
          SENDER_CLUSTER_ID_TABLE_MODEL,
          LAST_HANDSHAKE_TIME_TABLE_MODEL,
          LAST_TRANSFER_TIME_TABLE_MODEL);

  private final PlanTester planTester = new PlanTester();

  @Test
  public void testShowReceiversRewrite() {
    final LogicalQueryPlan logicalQueryPlan = planTester.createPlan("show receivers");
    assertPlan(
        logicalQueryPlan,
        output(
            infoSchemaTableScan(
                "information_schema.receivers", Optional.empty(), RECEIVERS_COLUMNS)));
  }

  @Test
  public void testSelectReceivers() {
    final LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("select * from information_schema.receivers");
    assertPlan(
        logicalQueryPlan,
        output(
            infoSchemaTableScan(
                "information_schema.receivers", Optional.empty(), RECEIVERS_COLUMNS)));
  }
}
