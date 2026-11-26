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

package org.apache.iotdb.db.queryengine.udf;

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;

/** Internal query result holding {@link IQueryExecution} and cleanup metadata. */
public final class InternalQueryResult implements AutoCloseable {

  private final IQueryExecution queryExecution;
  private final IClientSession internalSession;
  private final long statementId;
  private final long queryId;
  private final String sql;

  public InternalQueryResult(
      IQueryExecution queryExecution,
      IClientSession internalSession,
      long statementId,
      long queryId,
      String sql) {
    this.queryExecution = queryExecution;
    this.internalSession = internalSession;
    this.statementId = statementId;
    this.queryId = queryId;
    this.sql = sql;
  }

  public IQueryExecution getQueryExecution() {
    return queryExecution;
  }

  public long getQueryId() {
    return queryId;
  }

  public DatasetHeader getDatasetHeader() {
    return queryExecution.getDatasetHeader();
  }

  @Override
  public void close() {
    ClientRPCServiceImpl.clearUp(internalSession, statementId, queryId, () -> sql, null);
  }
}
