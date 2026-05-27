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

package org.apache.iotdb.db.queryengine.plan.planner.node.write;

import org.apache.iotdb.calc.utils.Base32ObjectPath;
import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Test;

public class ObjectNodeTest {

  @Test
  public void testGenValueInsertRowNodeKeepsTableNameAsSinglePathNode() throws Exception {
    String tableName = "root.table1";
    ObjectNode objectNode =
        new ObjectNode(true, 0, new byte[] {1}, getObjectPath(tableName, 1, "file"));

    RelationalInsertRowNode insertRowNode = objectNode.genValueInsertRowNode();

    Assert.assertEquals(tableName, insertRowNode.getTargetPath().getFullPath());
    Assert.assertEquals(1, insertRowNode.getTargetPath().getNodeLength());
  }

  @Test
  public void testSerializeFailsWhenObjectFileIsMissing() {
    ObjectNode objectNode =
        new ObjectNode(true, 0, 1, getObjectPath("missing_table", 987654321, "missing_file"));

    SerializationRunTimeException exception =
        Assert.assertThrows(SerializationRunTimeException.class, objectNode::serialize);

    Assert.assertTrue(exception.getCause().getMessage().contains("Failed to read object file"));
  }

  private IObjectPath getObjectPath(String tableName, long time, String measurement) {
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {tableName, "d1"});
    return new Base32ObjectPath(Integer.MAX_VALUE, time, deviceID, measurement);
  }
}
