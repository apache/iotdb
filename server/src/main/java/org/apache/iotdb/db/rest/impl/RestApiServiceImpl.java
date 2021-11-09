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

package org.apache.iotdb.db.rest.impl;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.rest.RestApiService;
import org.apache.iotdb.db.rest.handler.AuthorizationHandler;
import org.apache.iotdb.db.rest.handler.ExceptionHandler;
import org.apache.iotdb.db.rest.handler.PhysicalPlanConstructionHandler;
import org.apache.iotdb.db.rest.handler.QueryDataSetHandler;
import org.apache.iotdb.db.rest.handler.RequestValidationHandler;
import org.apache.iotdb.db.rest.model.ExecutionStatus;
import org.apache.iotdb.db.rest.model.InsertTabletRequest;
import org.apache.iotdb.db.rest.model.SQL;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.io.IOException;
import java.sql.SQLException;

public class RestApiServiceImpl extends RestApiService {

  protected final IPlanExecutor executor = new PlanExecutor(); // todo cluster
  protected final Planner planner = new Planner();

  public RestApiServiceImpl() throws QueryProcessException {}

  @Override
  public Response executeNonQueryStatement(SQL sql, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateSQL(sql);

      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(sql.getSql());
      Response response = AuthorizationHandler.checkAuthority(securityContext, physicalPlan);
      if (response != null) {
        return response;
      }
      return Response.ok()
          .entity(
              executor.processNonQuery(physicalPlan)
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

  private QueryDataSet getDataBySelect(PhysicalPlan physicalPlan)
      throws TException, StorageEngineException, QueryFilterOptimizationException,
          MetadataException, IOException, InterruptedException, SQLException,
          QueryProcessException {
    long queryId = QueryResourceManager.getInstance().assignQueryId(true);
    QueryContext context = new QueryContext(queryId);
    return executor.processQuery(physicalPlan, context);
  }

  @Override
  public Response executeQueryStatement(SQL sql, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateSQL(sql);

      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(sql.getSql());
      if (!(physicalPlan instanceof QueryPlan)) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }

      Response response = AuthorizationHandler.checkAuthority(securityContext, physicalPlan);
      if (response != null) {
        return response;
      }
      return QueryDataSetHandler.fillDateSet(
          getDataBySelect(physicalPlan), (QueryPlan) physicalPlan);
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

      Response response = AuthorizationHandler.checkAuthority(securityContext, insertTabletPlan);
      if (response != null) {
        return response;
      }

      return Response.ok()
          .entity(
              executeNonQuery(insertTabletPlan)
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

  private boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (!(plan instanceof SetSystemModePlan)
        && !(plan instanceof FlushPlan)
        && IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return executor.processNonQuery(plan);
  }
}
