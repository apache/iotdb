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
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iotdb.cluster.qp.executor.NonQueryExecutor;
import org.apache.iotdb.cluster.qp.executor.QueryMetadataExecutor;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementResp;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.iotdb.tsfile.read.common.Path;
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

  @Override
  public TSExecuteBatchStatementResp executeBatchStatement(TSExecuteBatchStatementReq req)
      throws TException {
    try {
      if (!checkLogin()) {
        LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN, null);
      }
      List<String> statements = req.getStatements();
      PhysicalPlan[] physicalPlans = new PhysicalPlan[statements.size()];
      int[] result = new int[statements.size()];
      String batchErrorMessage = "";
      boolean isAllSuccessful = true;

      /** find all valid physical plans **/
      for (int i = 0; i < statements.size(); i++) {
        try {
          PhysicalPlan plan = processor
              .parseSQLToPhysicalPlan(statements.get(i), zoneIds.get());
          plan.setProposer(username.get());

          /** if meet a query, handle all requests before the query request. **/
          if (plan.isQuery()) {
            int[] resultTemp = new int[i];
            PhysicalPlan[] physicalPlansTemp = new PhysicalPlan[i];
            System.arraycopy(result, 0, resultTemp, 0, i);
            System.arraycopy(physicalPlans, 0, physicalPlansTemp, 0, i);
            result = resultTemp;
            physicalPlans = physicalPlansTemp;
            BatchResult batchResult = new BatchResult(isAllSuccessful, batchErrorMessage, result);
            nonQueryExecutor.get().processBatch(physicalPlans, batchResult);
            return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
                "statement is query :" + statements.get(i), Arrays.stream(result).boxed().collect(
                    Collectors.toList()));
          }

          // check permissions
          List<Path> paths = plan.getPaths();
          if (!checkAuthorization(paths, plan)) {
            String errMessage = String.format("No permissions for this operation %s",
                plan.getOperatorType());
            result[i] = Statement.EXECUTE_FAILED;
            isAllSuccessful = false;
            batchErrorMessage = errMessage;
          } else {
            physicalPlans[i] = plan;
          }
        } catch (AuthException e) {
          LOGGER.error("meet error while checking authorization.", e);
          String errMessage = String.format("Uninitialized authorizer" + " beacuse %s",
              e.getMessage());
          result[i] = Statement.EXECUTE_FAILED;
          isAllSuccessful = false;
          batchErrorMessage = errMessage;
        } catch (Exception e) {
          String errMessage = String.format("Fail to generate physcial plan" + "%s beacuse %s",
              statements.get(i), e.getMessage());
          result[i] = Statement.EXECUTE_FAILED;
          isAllSuccessful = false;
          batchErrorMessage = errMessage;
        }
      }

      BatchResult batchResult = new BatchResult(isAllSuccessful, batchErrorMessage, result);
      nonQueryExecutor.get().processBatch(physicalPlans, batchResult);
      batchErrorMessage = batchResult.batchErrorMessage;
      isAllSuccessful = batchResult.isAllSuccessful;

      if (isAllSuccessful) {
        return getTSBathExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS,
            "Execute batch statements successfully", Arrays.stream(result).boxed().collect(
                Collectors.toList()));
      } else {
        return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, batchErrorMessage,
            Arrays.stream(result).boxed().collect(
                Collectors.toList()));
      }
    } catch (Exception e) {
      LOGGER.error("{}: error occurs when executing statements", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage(), null);
    }
  }

  /**
   * Present batch results.
   */
  public class BatchResult {

    private boolean isAllSuccessful;
    private String batchErrorMessage;
    private int[] result;

    public BatchResult(boolean isAllSuccessful, String batchErrorMessage, int[] result) {
      this.isAllSuccessful = isAllSuccessful;
      this.batchErrorMessage = batchErrorMessage;
      this.result = result;
    }

    public boolean isAllSuccessful() {
      return isAllSuccessful;
    }

    public void setAllSuccessful(boolean allSuccessful) {
      isAllSuccessful = allSuccessful;
    }

    public String getBatchErrorMessage() {
      return batchErrorMessage;
    }

    public void setBatchErrorMessage(String batchErrorMessage) {
      this.batchErrorMessage = batchErrorMessage;
    }

    public int[] getResult() {
      return result;
    }

    public void setResult(int[] result) {
      this.result = result;
    }
  }

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
  protected Set<String> getAllStorageGroups() throws InterruptedException {
    return queryMetadataExecutor.get().processStorageGroupQuery();
  }

  @Override
  protected List<List<String>> getTimeSeriesForPath(String path)
      throws PathErrorException, InterruptedException, ProcessorException {
    return queryMetadataExecutor.get().processTimeSeriesQuery(path);
  }

  @Override
  protected String getMetadataInString()
      throws InterruptedException, PathErrorException, ProcessorException {
    return queryMetadataExecutor.get().processMetadataInStringQuery();
  }
}
