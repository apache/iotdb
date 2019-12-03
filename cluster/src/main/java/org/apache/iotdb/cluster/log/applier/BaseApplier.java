/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseApplier use QueryProcessExecutor to execute PhysicalPlans.
 */
abstract class BaseApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(BaseApplier.class);

  private QueryProcessExecutor queryExecutor;

  void applyPhysicalPlan(PhysicalPlan plan) throws QueryProcessException {
    if (!plan.isQuery()) {
      getQueryExecutor().processNonQuery(plan);
    } else {
      // TODO-Cluster support more types of logs
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
