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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.NodeRef;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.TransformOperator;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;

import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.ZoneId;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class TransformOperatorResourceTest {

  @Test
  public void testOperatorsInSameDriverHaveIndependentUDFResourceIds() throws Exception {
    DriverContext driverContext = new DriverContext();
    driverContext.setDriverTaskID(
        new DriverTaskId(
            new FragmentInstanceId(new PlanFragmentId(new QueryId("udf_resources"), 0), "0"), 0));
    TransformOperator first = newTransform(driverContext, 1);
    TransformOperator second = newTransform(driverContext, 2);
    try {
      Field resourceId = TransformOperator.class.getDeclaredField("udtfQueryId");
      resourceId.setAccessible(true);
      assertNotEquals(resourceId.get(first), resourceId.get(second));
    } finally {
      first.close();
      second.close();
    }
  }

  private TransformOperator newTransform(DriverContext driverContext, int operatorId)
      throws Exception {
    TimeSeriesOperand input =
        new TimeSeriesOperand(new PartialPath("root.review.d.s1"), TSDataType.INT64);
    return new TransformOperator(
        driverContext.addOperatorContext(
            operatorId, new PlanNodeId("transform" + operatorId), "TransformOperator"),
        mock(Operator.class),
        ImmutableList.of(TSDataType.INT64),
        ImmutableMap.of(input.getExpressionString(), ImmutableList.of(new InputLocation(0, 0))),
        new Expression[] {input},
        true,
        ZoneId.systemDefault(),
        ImmutableMap.of(NodeRef.of(input), TSDataType.INT64),
        true);
  }
}
