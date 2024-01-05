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
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

public class PipeStatementExceptionVisitor extends StatementVisitor<TSStatus, Exception> {
  @Override
  public TSStatus visitNode(StatementNode node, Exception context) {
    return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
        .setMessage(context.getMessage());
  }

  @Override
  public TSStatus visitLoadFile(LoadTsFileStatement loadTsFileStatement, Exception context) {
    if (context instanceof VerifyMetadataException) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitLoadFile(loadTsFileStatement, context);
  }
}
