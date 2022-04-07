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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.rest.GrafanaApiService;
import org.apache.iotdb.db.protocol.rest.NotFoundException;
import org.apache.iotdb.db.protocol.rest.handler.AuthorizationHandler;
import org.apache.iotdb.db.protocol.rest.handler.ExceptionHandler;
import org.apache.iotdb.db.protocol.rest.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.rest.handler.RequestValidationHandler;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.model.ExpressionRequest;
import org.apache.iotdb.db.protocol.rest.model.SQL;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByLevelDataSet;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import com.google.common.base.Joiner;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;

public class GrafanaApiServiceImpl extends GrafanaApiService {

  private final ServiceProvider serviceProvider = IoTDB.serviceProvider;
  private final AuthorizationHandler authorizationHandler;

  private final long timePrecision; // the default timestamp precision is ms

  public GrafanaApiServiceImpl() throws QueryProcessException {
    authorizationHandler = new AuthorizationHandler();

    switch (IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision()) {
      case "ns":
        timePrecision = 1000000;
        break;
      case "us":
        timePrecision = 1000;
        break;
      default:
        timePrecision = 1;
    }
  }

  @Override
  public Response variables(SQL sql, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateSQL(sql);

      PhysicalPlan physicalPlan = serviceProvider.getPlanner().parseSQLToPhysicalPlan(sql.getSql());
      if (!(physicalPlan instanceof ShowPlan) && !(physicalPlan instanceof QueryPlan)) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }

      Response response = authorizationHandler.checkAuthority(securityContext, physicalPlan);
      if (response != null) {
        return response;
      }

      final long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
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
        return QueryDataSetHandler.fillGrafanaVariablesResult(queryDataSet, physicalPlan);
      } finally {
        ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
      }
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  @Override
  public Response expression(ExpressionRequest expressionRequest, SecurityContext securityContext)
      throws NotFoundException {
    try {
      RequestValidationHandler.validateExpressionRequest(expressionRequest);

      final String expression = Joiner.on(",").join(expressionRequest.getExpression());
      final String prefixPaths = Joiner.on(",").join(expressionRequest.getPrefixPath());
      final long startTime =
          (long) (expressionRequest.getStartTime().doubleValue() * timePrecision);
      final long endTime = (long) (expressionRequest.getEndTime().doubleValue() * timePrecision);
      String sql =
          "select "
              + expression
              + " from "
              + prefixPaths
              + " where timestamp>="
              + startTime
              + " and timestamp<= "
              + endTime;
      if (StringUtils.isNotEmpty(expressionRequest.getCondition())) {
        sql += " and " + expressionRequest.getCondition();
      }
      if (StringUtils.isNotEmpty(expressionRequest.getControl())) {
        sql += " " + expressionRequest.getControl();
      }

      PhysicalPlan physicalPlan =
          serviceProvider.getPlanner().parseSQLToGrafanaQueryPlan(sql, ZoneId.systemDefault());

      Response response = authorizationHandler.checkAuthority(securityContext, physicalPlan);
      if (response != null) {
        return response;
      }

      final long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
      try {
        QueryContext queryContext =
            serviceProvider.genQueryContext(
                queryId,
                physicalPlan.isDebug(),
                System.currentTimeMillis(),
                sql,
                IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
        QueryDataSet queryDataSet =
            serviceProvider.createQueryDataSet(
                queryContext, physicalPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);

        if (queryDataSet instanceof GroupByLevelDataSet) {
          return QueryDataSetHandler.fillGroupByLevelDataSet(queryDataSet, 0, timePrecision);
        } else {
          return QueryDataSetHandler.fillDataSetWithTimestamps(
              queryDataSet, (QueryPlan) physicalPlan, 0, timePrecision);
        }
      } finally {
        ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
      }
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  @Override
  public Response login(SecurityContext securityContext) throws NotFoundException {
    return Response.ok()
        .entity(
            new ExecutionStatus()
                .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                .message(TSStatusCode.SUCCESS_STATUS.name()))
        .build();
  }
}
