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
import org.apache.iotdb.cluster.qp.executor.NonQueryExecutor;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed version of PRC implementation
 */
public class TSServiceClusterImpl extends TSServiceImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceClusterImpl.class);

  private ThreadLocal<NonQueryExecutor> nonQueryExecutor = new ThreadLocal<>();

  public TSServiceClusterImpl() throws IOException {
    super();
  }

  @Override
  public void initClusterService() {
    nonQueryExecutor.set(new NonQueryExecutor());
    nonQueryExecutor.get().init();
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
  public void closeClusterService() {
    nonQueryExecutor.get().shutdown();
  }
}
