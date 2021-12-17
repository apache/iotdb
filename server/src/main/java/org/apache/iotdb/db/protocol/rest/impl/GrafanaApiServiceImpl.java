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
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.service.basic.BasicServiceProvider;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class GrafanaApiServiceImpl extends GrafanaApiService {

  // todo cluster
  protected final IPlanExecutor executor;
  protected final Planner planner;
  protected final BasicServiceProvider basicServiceProvider;
  protected final AuthorizationHandler authorizationHandler;

  private float timePrecision; // the timestamp Precision is default ms

  public GrafanaApiServiceImpl() throws QueryProcessException {
    executor = new PlanExecutor();
    planner = new Planner();
    basicServiceProvider = new BasicServiceProvider();
    authorizationHandler = new AuthorizationHandler(basicServiceProvider);
    String timestampPrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    timePrecision = 1;
    switch (timestampPrecision) {
      case "ns":
        timePrecision = timePrecision * 1000000;
        break;
      case "us":
        timePrecision = timePrecision * 1000;
        break;
      case "s":
        timePrecision = timePrecision / 1000;
        break;
    }
  }

  @Override
  public Response variables(SQL sql, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateSQL(sql);

      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(sql.getSql());
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

      return QueryDataSetHandler.constructVariablesResult(
          QueryDataSetHandler.constructQueryDataSet(executor, physicalPlan), physicalPlan);
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  @Override
  public Response expression(ExpressionRequest expressionRequest, SecurityContext securityContext)
      throws NotFoundException {
    try {
      long startTime = (long) (expressionRequest.getStartTime().doubleValue() * timePrecision);
      long endTime = (long) (expressionRequest.getEndTime().doubleValue() * timePrecision);
      String prefixPaths = Joiner.on(",").join(expressionRequest.getPrefixPath());
      String expression = Joiner.on(",").join(expressionRequest.getExpression());

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
      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(sql);
      Response response = authorizationHandler.checkAuthority(securityContext, physicalPlan);
      if (response != null) {
        return response;
      }

      return QueryDataSetHandler.fillDateSet(
          QueryDataSetHandler.constructQueryDataSet(executor, physicalPlan),
          (QueryPlan) physicalPlan);
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }
}
