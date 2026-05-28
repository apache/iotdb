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

package org.apache.iotdb.confignode.manager.pipe.receiver.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewPropertiesPlan;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class PipeConfigPhysicalPlanTSStatusVisitorTest {

  @Test
  public void testWritableViewTableIncompatibleIsUserConflict() {
    final SetWritableViewPropertiesPlan plan =
        new SetWritableViewPropertiesPlan(
            "view_db", "view_table", Collections.emptyMap(), "src_db", "src_table");
    final TSStatus status =
        new TSStatus(TSStatusCode.TABLE_INCOMPATIBLE.getStatusCode())
            .setMessage("Source table 'view_db.src_table' is not a base table.");

    final TSStatus result = new PipeConfigPhysicalPlanTSStatusVisitor().process(plan, status);

    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode(), result.getCode());
    Assert.assertEquals(status.getMessage(), result.getMessage());
  }
}
