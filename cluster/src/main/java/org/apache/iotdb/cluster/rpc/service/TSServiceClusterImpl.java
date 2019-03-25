/**
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
package org.apache.iotdb.cluster.rpc.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;
import org.apache.iotdb.cluster.qp.executor.NonQueryExecutor;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSHandleIdentifier;
import org.apache.iotdb.service.rpc.thrift.TSOperationHandle;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSServiceClusterImpl extends TSServiceImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceClusterImpl.class);

  private NonQueryExecutor nonQueryExecutor = new NonQueryExecutor();

  private ThreadLocal<String> username = new ThreadLocal<>();
  private ThreadLocal<ZoneId> zoneIds = new ThreadLocal<>();

  public TSServiceClusterImpl() throws IOException {
    super();
  }

  //TODO
  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) throws TException {
    throw new TException("not support");
  }

  //TODO

  /**
   * Judge whether the statement is ADMIN COMMAND and if true, executeWithGlobalTimeFilter it.
   *
   * @param statement command
   * @return true if the statement is ADMIN COMMAND
   * @throws IOException exception
   */
  @Override
  public boolean execAdminCommand(String statement) throws IOException {
    throw new IOException("exec admin command not support");
  }

  //TODO
  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) throws TException {
    throw new TException("query not support");
  }


  //TODO
  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) throws TException {
    throw new TException("not support");
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(PhysicalPlan plan) {
    List<Path> paths = plan.getPaths();

    try {
      if (!checkAuthorization(paths, plan)) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            "No permissions for this operation " + plan.getOperatorType());
      }
    } catch (AuthException e) {
      LOGGER.error("meet error while checking authorization.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Uninitialized authorizer " + e.getMessage());
    }
    // TODO
    // In current version, we only return OK/ERROR
    // Do we need to add extra information of executive condition
    boolean execRet;
    try {
      execRet = nonQueryExecutor.processNonQuery(plan);
    } catch (Exception e) {
      LOGGER.error("meet error while processing non-query.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
    TS_StatusCode statusCode = execRet ? TS_StatusCode.SUCCESS_STATUS : TS_StatusCode.ERROR_STATUS;
    String msg = execRet ? "Execute successfully" : "Execute statement error.";
    TSExecuteStatementResp resp = getTSExecuteStatementResp(statusCode, msg);
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()));
    TSOperationHandle operationHandle;
    operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  //TODO
  public void handleClientExit() throws TException {
    closeOperation(null);
    closeSession(null);
  }

  //TODO
  @Override
  public ServerProperties getProperties() throws TException {
    throw new TException("not support");
  }
}
