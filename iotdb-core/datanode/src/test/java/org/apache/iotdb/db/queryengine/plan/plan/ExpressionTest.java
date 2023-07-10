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

package org.apache.iotdb.db.queryengine.plan.plan;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExpressionTest {

  @Test
  public void testIsMappable() {
    try {
      TimeSeriesOperand input = new TimeSeriesOperand(new PartialPath("root.sg.a"));
      FunctionExpression masterRepair =
          new FunctionExpression(
              "Master_Repair", new LinkedHashMap<>(), Collections.singletonList(input));
      FunctionExpression abs =
          new FunctionExpression(
              "abs", new LinkedHashMap<>(), Collections.singletonList(masterRepair));
      FunctionExpression rawAbs =
          new FunctionExpression("abs", new LinkedHashMap<>(), Collections.singletonList(input));
      FunctionExpression masterRepairAbs =
          new FunctionExpression(
              "Master_Repair", new LinkedHashMap<>(), Collections.singletonList(rawAbs));
      Map<NodeRef<Expression>, TSDataType> expressionTypes = new HashMap<>();
      expressionTypes.put(NodeRef.of(input), TSDataType.INT32);
      expressionTypes.put(NodeRef.of(masterRepair), TSDataType.INT32);
      expressionTypes.put(NodeRef.of(rawAbs), TSDataType.INT32);
      Assert.assertTrue(input.isMappable(expressionTypes));
      Assert.assertFalse(masterRepair.isMappable(expressionTypes));
      // abs(M4(input)) is not mappable because MasterRepair is not mappable
      Assert.assertFalse(abs.isMappable(expressionTypes));
      Assert.assertFalse(masterRepairAbs.isMappable(expressionTypes));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
