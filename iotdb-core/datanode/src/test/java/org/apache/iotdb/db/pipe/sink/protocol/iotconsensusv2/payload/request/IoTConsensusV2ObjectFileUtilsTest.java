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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestType;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2ObjectFileUtils.ObjectFileDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;

import com.timecho.iotdb.calc.storageengine.dataregion.Base32ObjectPath;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IoTConsensusV2ObjectFileUtilsTest {

  @Test
  public void testCollectObjectFileDescriptorsKeepsLastDuplicate() {
    final IObjectPath objectPath = getObjectPath("root.table1", 1, "object_col");
    final Binary firstObjectValue = ObjectTypeUtils.generateObjectBinary(1, objectPath);
    final Binary lastObjectValue = ObjectTypeUtils.generateObjectBinary(2, objectPath);
    final InsertRowsNode insertRowsNode =
        new InsertRowsNode(
            new PlanNodeId("insertRows"),
            Arrays.asList(0, 1),
            Arrays.asList(getInsertRowNode(firstObjectValue), getInsertRowNode(lastObjectValue)));

    final List<ObjectFileDescriptor> descriptors =
        IoTConsensusV2ObjectFileUtils.collectObjectFileDescriptors(insertRowsNode);

    Assert.assertEquals(1, descriptors.size());
    Assert.assertEquals(2, descriptors.get(0).getObjectSize());
    Assert.assertEquals(objectPath.toString(), descriptors.get(0).getObjectPathString());
  }

  @Test
  public void testObjectFilePieceRequestTypeUsesTimechoPrivateRange() {
    Assert.assertTrue(IoTConsensusV2RequestType.TRANSFER_OBJECT_FILE_PIECE.getType() < 0);
  }

  private InsertRowNode getInsertRowNode(final Binary objectValue) {
    return new InsertRowNode(
        new PlanNodeId("insertRow"),
        null,
        false,
        new String[] {"object_col"},
        new TSDataType[] {TSDataType.OBJECT},
        1,
        new Object[] {objectValue},
        false);
  }

  private IObjectPath getObjectPath(
      final String tableName, final long time, final String measurement) {
    final IDeviceID deviceID =
        IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {tableName, "d1"});
    return new Base32ObjectPath(Integer.MAX_VALUE, time, deviceID, measurement);
  }
}
