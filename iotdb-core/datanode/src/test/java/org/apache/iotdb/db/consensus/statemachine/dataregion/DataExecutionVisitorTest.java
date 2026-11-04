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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

public class DataExecutionVisitorTest {

  @Test
  public void testDeleteFailureReturnsMessage() throws Exception {
    final DataRegion dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.sg");
    final IOException failure = new IOException("injected delete failure");
    Mockito.doThrow(failure)
        .when(dataRegion)
        .deleteByDevice(any(PartialPath.class), anyLong(), anyLong(), anyLong());

    final DeleteDataNode node =
        new DeleteDataNode(
            new PlanNodeId("delete-error-test"),
            Collections.singletonList(new PartialPath("root.sg.device.s1")),
            Long.MIN_VALUE,
            Long.MAX_VALUE);

    final TSStatus status = new DataExecutionVisitor().visitDeleteData(node, dataRegion);

    Assert.assertEquals(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(failure.getMessage(), status.getMessage());
    Mockito.verify(dataRegion).writeUnlock();
  }
}
