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

package org.apache.iotdb.db.pipe.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;

public class PipeStatementTSStatusVisitor extends StatementVisitor<TSStatus, TSStatus> {
  @Override
  public TSStatus visitNode(StatementNode node, TSStatus context) {
    return context;
  }

  @Override
  public TSStatus visitInsertTablet(InsertTabletStatement insertTabletStatement, TSStatus context) {
    return visitInsertBase(insertTabletStatement, context);
  }

  @Override
  public TSStatus visitInsertRow(InsertRowStatement insertRowStatement, TSStatus context) {
    return visitInsertBase(insertRowStatement, context);
  }

  @Override
  public TSStatus visitInsertRows(InsertRowsStatement insertRowsStatement, TSStatus context) {
    return visitInsertBase(insertRowsStatement, context);
  }

  @Override
  public TSStatus visitInsertMultiTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, TSStatus context) {
    return visitInsertBase(insertMultiTabletsStatement, context);
  }

  private TSStatus visitInsertBase(InsertBaseStatement insertBaseStatement, TSStatus context) {
    if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode());
    }
    return visitStatement(insertBaseStatement, context);
  }
}
