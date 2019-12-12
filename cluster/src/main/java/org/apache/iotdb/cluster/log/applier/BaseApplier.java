/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.applier;

import java.util.List;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseApplier use QueryProcessExecutor to execute PhysicalPlans.
 */
abstract class BaseApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(BaseApplier.class);

  private MetaGroupMember metaGroupMember;
  private QueryProcessExecutor queryExecutor;

  BaseApplier(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  void applyPhysicalPlan(PhysicalPlan plan) throws QueryProcessException {
    if (plan instanceof InsertPlan) {
      try {
        getQueryExecutor().processNonQuery(plan);
      } catch (QueryProcessException e) {
        if (e.getCause() instanceof PathNotExistException) {
          logger.debug("Timeseries is not found locally, try pulling it from another group: {}",
              e.getCause().getMessage());
          InsertPlan insertPlan = ((InsertPlan) plan);
          try {
            List<MeasurementSchema> schemas = metaGroupMember
                .pullTimeSeriesSchemas(insertPlan.getDeviceId());
            for (MeasurementSchema schema : schemas) {
              SchemaUtils.registerTimeseries(schema);
            }
          } catch (StorageGroupNotSetException e1) {
            throw new QueryProcessException(e1);
          }
          getQueryExecutor().processNonQuery(plan);
        } else {
          throw e;
        }
      }
    } else if (!plan.isQuery()) {
      getQueryExecutor().processNonQuery(plan);
    } else {
      // TODO-Cluster#348 support more types of logs
      logger.error("Unsupported physical plan: {}", plan);
    }
  }

  private QueryProcessExecutor getQueryExecutor() {
    if (queryExecutor == null) {
      queryExecutor = new QueryProcessExecutor();
    }
    return queryExecutor;
  }
}
