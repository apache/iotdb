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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe;

import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ShowPipeTaskTest {

  @Test
  public void testBuildTSBlockWritesRecentFailures() throws Exception {
    final TShowPipeInfo failedPipe = createPipeInfo("failed_pipe");
    final Map<String, Long> recentFailures = new HashMap<>();
    recentFailures.put("network_timeout", 10L);
    recentFailures.put("memory_timeout", 15L);
    failedPipe.setRecentFailures(recentFailures);
    final TShowPipeInfo normalPipe = createPipeInfo("normal_pipe");

    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ShowPipeTask.buildTSBlock(Arrays.asList(failedPipe, normalPipe), future);

    final ConfigTaskResult result = future.get();
    final TsBlock resultSet = result.getResultSet();

    assertEquals(TSStatusCode.SUCCESS_STATUS, result.getStatusCode());
    assertEquals(
        ColumnHeaderConstant.RECENT_FAILURES, result.getResultSetHeader().getRespColumns().get(9));
    assertEquals(2, resultSet.getPositionCount());
    assertEquals(
        "{memory_timeout=15, network_timeout=10}", resultSet.getColumn(9).getBinary(0).toString());
    assertEquals("{}", resultSet.getColumn(9).getBinary(1).toString());
  }

  private TShowPipeInfo createPipeInfo(final String pipeName) {
    return new TShowPipeInfo(
        pipeName,
        1L,
        "RUNNING",
        "{source=iotdb-source}",
        "{processor=do-nothing-processor}",
        "{sink=iotdb-thrift-sink}",
        "");
  }
}
