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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class NodeRefTest {

  @Test
  public void testHashCode() {
    ConstantOperand constantOperand1 = new ConstantOperand(TSDataType.INT64, "2");
    ConstantOperand constantOperand2 = new ConstantOperand(TSDataType.TEXT, "2");
    NodeRef<Expression> constant1 = NodeRef.of(constantOperand1);
    NodeRef<Expression> constant2 = NodeRef.of(constantOperand2);
    NodeRef<Expression> constant3 = NodeRef.of(constantOperand1);
    Map<NodeRef<Expression>, String> map = new HashMap<>();
    map.put(constant1, "test");
    Assert.assertNull(map.get(constant2));
    Assert.assertEquals("test", map.get(constant3));
  }
}
