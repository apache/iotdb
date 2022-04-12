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

package org.apache.iotdb.db.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.Executor.ISchemaQueryExecutor;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegion;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaRegionStateMachine extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionStateMachine.class);

  private final SchemaRegion schemaRegion;

  public SchemaRegionStateMachine(SchemaRegion schemaRegion) {
    this.schemaRegion = schemaRegion;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  protected TSStatus write(FragmentInstance fragmentInstance) {
    logger.info("Execute write plan in SchemaRegionStateMachine");

    PlanNode planNode = fragmentInstance.getFragment().getRoot();
    if (planNode instanceof ISchemaQueryExecutor) {
      try {
        ((ISchemaQueryExecutor) planNode).executeOn(schemaRegion);
      } catch (MetadataException e) {
        logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
        return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
      } catch (QueryProcessException e) {
        logger.error("meet error while processing non-query. ", e);
        return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
      } catch (Exception e) {
        logger.error("{}: server Internal Error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
        return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
    } else {
      String msg = "PlanNode does not belong schema query.";
      logger.error(msg);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, msg);
    }
  }

  @Override
  protected DataSet read(FragmentInstance fragmentInstance) {
    // TODO Set an example, maybe read operation is different from write
    logger.info("Execute read plan in SchemaRegionStateMachine");
    PlanNode planNode = fragmentInstance.getFragment().getRoot();
    if (planNode instanceof ISchemaQueryExecutor) {
      ((ISchemaQueryExecutor) planNode).queryOn(schemaRegion);
    }
    return null;
  }
}
