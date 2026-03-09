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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;

public class LoadConvertedInsertTabletStatementTSStatusVisitor
    extends StatementVisitor<TSStatus, TSStatus> {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public TSStatus visitNode(final StatementNode node, final TSStatus context) {
    return context;
  }

  @Override
  public TSStatus visitInsertTablet(
      final InsertTabletStatement insertTabletStatement, final TSStatus context) {
    return visitInsertBase(insertTabletStatement, context);
  }

  @Override
  public TSStatus visitInsertMultiTablets(
      final InsertMultiTabletsStatement insertMultiTabletsStatement, final TSStatus context) {
    return visitInsertBase(insertMultiTabletsStatement, context);
  }

  @Override
  public TSStatus visitInsertBase(
      final InsertBaseStatement insertBaseStatement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()
        || context.getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
      return new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.OUT_OF_TTL.getStatusCode()) {
      return new TSStatus(TSStatusCode.LOAD_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
        && (context.getMessage().contains(DataTypeMismatchException.REGISTERED_TYPE_STRING)
            && config.isEnablePartialInsert())) {
      return new TSStatus(TSStatusCode.LOAD_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitStatement(insertBaseStatement, context);
  }
}
