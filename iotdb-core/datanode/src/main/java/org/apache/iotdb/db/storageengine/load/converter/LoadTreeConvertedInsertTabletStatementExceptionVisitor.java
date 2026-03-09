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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

public class LoadTreeConvertedInsertTabletStatementExceptionVisitor
    extends StatementVisitor<TSStatus, Exception> {

  @Override
  public TSStatus visitNode(final StatementNode node, final Exception context) {
    if (context instanceof AccessDeniedException) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
        .setMessage(context.getMessage());
  }

  @Override
  public TSStatus visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final Exception context) {
    if (context instanceof LoadRuntimeOutOfMemoryException) {
      return new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context instanceof SemanticException) {
      return new TSStatus(TSStatusCode.LOAD_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitStatement(loadTsFileStatement, context);
  }
}
