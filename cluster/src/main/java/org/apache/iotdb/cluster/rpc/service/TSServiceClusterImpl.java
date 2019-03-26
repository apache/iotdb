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

import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.io.IOException;
import java.time.ZoneId;
import org.apache.iotdb.cluster.qp.executor.NonQueryExecutor;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed version of PRC implementation
 */
public class TSServiceClusterImpl extends TSServiceImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceClusterImpl.class);

  /**
   *
   */
  private BoltCliClientService cliClientService = new BoltCliClientService();
  /**
   *
   */
  private NonQueryExecutor nonQueryExecutor = new NonQueryExecutor(cliClientService);

  private ThreadLocal<String> username = new ThreadLocal<>();
  private ThreadLocal<ZoneId> zoneIds = new ThreadLocal<>();

  public TSServiceClusterImpl() throws IOException {
    super();
    cliClientService.init(new CliOptions());
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
  public boolean executeNonQuery(PhysicalPlan plan) throws ProcessorException {
    return nonQueryExecutor.processNonQuery(plan);
  }

  //TODO
  public void handleClientExit() throws TException {
    cliClientService.shutdown();
    closeOperation(null);
    closeSession(null);
  }

  //TODO
  @Override
  public ServerProperties getProperties() throws TException {
    throw new TException("not support");
  }
}
