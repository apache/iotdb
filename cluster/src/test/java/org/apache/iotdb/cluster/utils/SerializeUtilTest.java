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

package org.apache.iotdb.cluster.utils;

import static org.junit.Assert.assertEquals;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.junit.Test;

public class SerializeUtilTest {

  @Test
  public void testStrToNode() {
    for (int i = 0; i < 10; i++) {
      Node node = TestUtils.getNode(i);
      String nodeStr = node.toString();
      Node fromStr = SerializeUtils.stringToNode(nodeStr);
      assertEquals(node, fromStr);
    }
  }
}
