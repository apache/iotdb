/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.impl;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.rest.RestApiService;
import org.apache.iotdb.db.protocol.rest.handler.AuthorizationHandler;
import org.apache.iotdb.db.protocol.rest.handler.ExceptionHandler;
import org.apache.iotdb.db.protocol.rest.handler.PhysicalPlanConstructionHandler;
import org.apache.iotdb.db.protocol.rest.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.rest.handler.RequestValidationHandler;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.model.SQL;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.time.ZoneId;

public class RestApiServiceImpl extends RestApiService {

  public static ServiceProvider serviceProvider = IoTDB.serviceProvider;

  private final Planner planner;
  private final AuthorizationHandler authorizationHandler;

  private final Integer defaultQueryRowLimit;

  public RestApiServiceImpl() throws QueryProcessException {
    planner = serviceProvider.getPlanner();
    authorizationHandler = new AuthorizationHandler(serviceProvider);

    defaultQueryRowLimit =
        IoTDBRestServiceDescriptor.getInstance().getConfig().getRestQueryDefaultRowSizeLimit();
  }

  @Override
  public Response executeNonQueryStatement(SQL sql, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateSQL(sql);

      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(sql.getSql());
      Response response = authorizationHandler.checkAuthority(securityContext, physicalPlan);
      if (response != null) {
        return response;
      }

      return Response.ok()
          .entity(
              serviceProvider.executeNonQuery(physicalPlan)
                  ? new ExecutionStatus()
                      .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                      .message(TSStatusCode.SUCCESS_STATUS.name())
                  : new ExecutionStatus()
                      .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                      .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
          .build();
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  @Override
  public Response executeQueryStatement(SQL sql, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateSQL(sql);

      PhysicalPlan physicalPlan =
          planner.parseSQLToRestQueryPlan(sql.getSql(), ZoneId.systemDefault());
      if (!(physicalPlan instanceof QueryPlan)) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }
      QueryPlan queryPlan = (QueryPlan) physicalPlan;

      Response response = authorizationHandler.checkAuthority(securityContext, queryPlan);
      if (response != null) {
        return response;
      }

      // set max row limit to avoid OOM
      Integer rowLimitInRequest = sql.getRowLimit();
      if (rowLimitInRequest == null) {
        rowLimitInRequest = defaultQueryRowLimit;
      }
      int rowLimitInQueryPlan = queryPlan.getRowLimit();
      if (rowLimitInQueryPlan <= 0) {
        rowLimitInQueryPlan = defaultQueryRowLimit;
      }
      final int actualRowSizeLimit = Math.min(rowLimitInRequest, rowLimitInQueryPlan);

      final long queryId = QueryResourceManager.getInstance().assignQueryId(true);
      try {
        QueryContext queryContext =
            serviceProvider.genQueryContext(
                queryId,
                physicalPlan.isDebug(),
                System.currentTimeMillis(),
                sql.getSql(),
                IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
        QueryDataSet queryDataSet =
            serviceProvider.createQueryDataSet(
                queryContext, physicalPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
        return QueryDataSetHandler.fillDateSet(queryDataSet, queryPlan, actualRowSizeLimit);
      } finally {
        ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
      }
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  @Override
  public Response insertTablet(
      InsertTabletRequest insertTabletRequest, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateInsertTabletRequest(insertTabletRequest);

      InsertTabletPlan insertTabletPlan =
          PhysicalPlanConstructionHandler.constructInsertTabletPlan(insertTabletRequest);

      Response response = authorizationHandler.checkAuthority(securityContext, insertTabletPlan);
      if (response != null) {
        return response;
      }

      return Response.ok()
          .entity(
              serviceProvider.executeNonQuery(insertTabletPlan)
                  ? new ExecutionStatus()
                      .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                      .message(TSStatusCode.SUCCESS_STATUS.name())
                  : new ExecutionStatus()
                      .code(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode())
                      .message(TSStatusCode.WRITE_PROCESS_ERROR.name()))
          .build();
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }
}
