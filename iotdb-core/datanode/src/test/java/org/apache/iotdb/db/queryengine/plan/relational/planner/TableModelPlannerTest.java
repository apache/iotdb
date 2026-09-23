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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableModelPlannerTest {

  @Test
  public void testSetRedirectInfoForInsertRows() {
    final InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    insertRowsStatement.setInsertRowStatementList(Collections.emptyList());
    final Analysis analysis =
        new Analysis(new InsertRows(insertRowsStatement, null), Collections.emptyMap());
    final TEndPoint localEndPoint = new TEndPoint("127.0.0.1", 6667);
    final TEndPoint remoteEndPoint = new TEndPoint("127.0.0.2", 6667);
    analysis.setRedirectNodeList(Arrays.asList(localEndPoint, remoteEndPoint));
    final TSStatus status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);

    createPlanner().setRedirectInfo(analysis, localEndPoint, status);

    Assert.assertEquals(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), status.getCode());
    final List<TSStatus> subStatus = status.getSubStatus();
    Assert.assertEquals(2, subStatus.size());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), subStatus.get(0).getCode());
    Assert.assertFalse(subStatus.get(0).isSetRedirectNode());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), subStatus.get(1).getCode());
    Assert.assertEquals(remoteEndPoint, subStatus.get(1).getRedirectNode());
  }

  private static TableModelPlanner createPlanner() {
    return new TableModelPlanner(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        null,
        Collections.emptyList(),
        Collections.emptyMap(),
        null);
  }
}
