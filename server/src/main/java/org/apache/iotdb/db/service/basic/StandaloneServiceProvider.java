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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageEngineReadonlyException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.query.context.QueryContext;

public class StandaloneServiceProvider extends ServiceProvider {

  public StandaloneServiceProvider() throws QueryProcessException {
    super(new PlanExecutor());
  }

  @Override
  public QueryContext genQueryContext(
      long queryId, boolean debug, long startTime, String statement, long timeout) {
    return new QueryContext(queryId, debug, startTime, statement, timeout);
  }

  @Override
  public boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    plan.checkIntegrity();
    if (!(plan instanceof SetSystemModePlan)
        && !(plan instanceof FlushPlan)
        && CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineReadonlyException();
    }
    return executor.processNonQuery(plan);
  }
}
