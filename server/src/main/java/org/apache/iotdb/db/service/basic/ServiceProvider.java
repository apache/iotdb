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

package org.apache.iotdb.db.service.basic;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.tracing.TracingManager;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public abstract class ServiceProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProvider.class);
  public static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);
  public static final Logger SLOW_SQL_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.SLOW_SQL_LOGGER_NAME);

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  public static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public static SessionManager SESSION_MANAGER = SessionManager.getInstance();

  // MPP: The following fields will be moved to Coordinator
  public static final QueryTimeManager QUERY_TIME_MANAGER = QueryTimeManager.getInstance();
  public static final TracingManager TRACING_MANAGER = TracingManager.getInstance();
  public static final QueryFrequencyRecorder QUERY_FREQUENCY_RECORDER =
      new QueryFrequencyRecorder(CONFIG);
  private final Planner planner;
  protected final IPlanExecutor executor;

  public Planner getPlanner() {
    return planner;
  }

  public IPlanExecutor getExecutor() {
    return executor;
  }

  public ServiceProvider(PlanExecutor executor) throws QueryProcessException {
    planner = new Planner();
    this.executor = executor;
  }

  public abstract QueryContext genQueryContext(
      long queryId, boolean debug, long startTime, String statement, long timeout);

  public abstract boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException;

  /** create QueryDataSet and buffer it for fetchResults */
  public QueryDataSet createQueryDataSet(
      QueryContext context, PhysicalPlan physicalPlan, int fetchSize)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException,
          IOException, MetadataException, SQLException, TException, InterruptedException {

    QueryDataSet queryDataSet = executor.processQuery(physicalPlan, context);
    queryDataSet.setFetchSize(fetchSize);
    return queryDataSet;
  }
}
