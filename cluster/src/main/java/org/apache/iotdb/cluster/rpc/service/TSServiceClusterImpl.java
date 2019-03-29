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
import java.util.Set;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.executor.NonQueryExecutor;
import org.apache.iotdb.cluster.qp.executor.QueryMetadataExecutor;
import org.apache.iotdb.cluster.rpc.MetadataType;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed version of PRC implementation
 */
public class TSServiceClusterImpl extends TSServiceImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceClusterImpl.class);

  private ThreadLocal<NonQueryExecutor> nonQueryExecutor = new ThreadLocal<>();
  private ThreadLocal<QueryMetadataExecutor> queryMetadataExecutor = new ThreadLocal<>();

  public TSServiceClusterImpl() throws IOException {
    super();
  }

  @Override
  public void initClusterService() {
    nonQueryExecutor.set(new NonQueryExecutor());
    nonQueryExecutor.get().init();
    queryMetadataExecutor.set(new QueryMetadataExecutor());
    queryMetadataExecutor.get().init();
  }

//  //TODO
//  @Override
//  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) throws TException {
//    throw new TException("not support");
//  }
//
//  //TODO
//
//  /**
//   * Judge whether the statement is ADMIN COMMAND and if true, executeWithGlobalTimeFilter it.
//   *
//   * @param statement command
//   * @return true if the statement is ADMIN COMMAND
//   * @throws IOException exception
//   */
//  @Override
//  public boolean execAdminCommand(String statement) throws IOException {
//    throw new IOException("exec admin command not support");
//  }
//
//  //TODO
//  @Override
//  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) throws TException {
//    throw new TException("query not support");
//  }
//
//
//  //TODO
//  @Override
//  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) throws TException {
//    throw new TException("not support");
//  }

  @Override
  public boolean executeNonQuery(PhysicalPlan plan) throws ProcessorException {
    return nonQueryExecutor.get().processNonQuery(plan);
  }

  /**
   * Close cluster service
   */
  @Override
  public void closeClusterService() {
    nonQueryExecutor.get().shutdown();
    queryMetadataExecutor.get().shutdown();
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) throws TException {
    TS_Status status;
    if (!checkLogin()) {
      LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      status = getErrorStatus(ERROR_NOT_LOGIN);
      return new TSFetchMetadataResp(status);
    }
    TSFetchMetadataResp resp = new TSFetchMetadataResp();
    switch (req.getType()) {
      case "SHOW_STORAGE_GROUP":
        try {
          Set<String> storageGroups = processMetadataQuery(MetadataType.STORAGE_GROUP);
          resp.setShowStorageGroups(storageGroups);
        } catch (InterruptedException e) {
          status = getErrorStatus(
              String.format("Failed to fetch storage groups' metadata because: %s", e));
          resp.setStatus(status);
          return resp;
        } catch (OutOfMemoryError outOfMemoryError) { // TODO OOME
          LOGGER.error("Failed to fetch storage groups' metadata", outOfMemoryError);
          status = getErrorStatus(
              String.format("Failed to fetch storage groups' metadata because: %s",
                  outOfMemoryError));
          resp.setStatus(status);
          return resp;
        }
        status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
        break;
      default:
        status = new TS_Status(TS_StatusCode.ERROR_STATUS);
        status
            .setErrorMessage(String.format("Unsuport fetch metadata operation %s", req.getType()));
        break;
    }
    resp.setStatus(status);
    return resp;
  }

  public Set<String> processMetadataQuery(MetadataType type)
      throws InterruptedException {
    return queryMetadataExecutor.get().processMetadataQuery(type);
  }
}
