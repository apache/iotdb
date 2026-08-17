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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShowPipeTaskTest {

  @Test
  public void testBuildTSBlockWritesRuntimeColumns() throws Exception {
    final TShowPipeInfo degradedPipe = createPipeInfo("degraded_pipe");
    degradedPipe.setIsDegraded(true);
    degradedPipe.setExceptionMessage("Authentication failed");
    final Map<String, Long> recentFailures = new HashMap<>();
    recentFailures.put("network_timeout", 10L);
    recentFailures.put("memory_timeout", 15L);
    degradedPipe.setRecentFailures(recentFailures);
    final TShowPipeInfo normalPipe = createPipeInfo("normal_pipe");
    normalPipe.setIsDegraded(false);
    final TShowPipeInfo unknownPipe = createPipeInfo("unknown_pipe");

    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ShowPipeTask.buildTSBlock(Arrays.asList(degradedPipe, normalPipe, unknownPipe), future);

    final ConfigTaskResult result = future.get();
    final TsBlock resultSet = result.getResultSet();

    assertEquals(TSStatusCode.SUCCESS_STATUS, result.getStatusCode());
    assertEquals(
        ColumnHeaderConstant.SUGGESTED_ACTION, result.getResultSetHeader().getRespColumns().get(7));
    assertEquals(
        ColumnHeaderConstant.IS_DEGRADED, result.getResultSetHeader().getRespColumns().get(10));
    assertEquals(
        ColumnHeaderConstant.RECENT_FAILURES, result.getResultSetHeader().getRespColumns().get(11));
    assertEquals(3, resultSet.getPositionCount());
    assertTrue(
        resultSet
            .getColumn(7)
            .getBinary(0)
            .toString()
            .contains("Please check whether the source's or sink's password is right."));
    assertTrue(resultSet.getColumn(10).getBoolean(0));
    assertFalse(resultSet.getColumn(10).getBoolean(1));
    assertTrue(resultSet.getColumn(10).isNull(2));
    assertEquals(
        "{memory_timeout=15, network_timeout=10}", resultSet.getColumn(11).getBinary(0).toString());
    assertEquals("{}", resultSet.getColumn(11).getBinary(1).toString());
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
