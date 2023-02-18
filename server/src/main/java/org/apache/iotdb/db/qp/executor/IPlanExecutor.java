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
package org.apache.iotdb.db.qp.executor;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.SQLException;

public interface IPlanExecutor {

  /**
   * process query plan of qp layer, construct queryDataSet.
   *
   * @param queryPlan QueryPlan
   * @return QueryDataSet
   */
  QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException, QueryFilterOptimizationException,
          QueryProcessException, MetadataException, SQLException, TException, InterruptedException;

  /**
   * Process Non-Query Physical plan, including insert/update/delete operation of
   * data/metadata/Privilege
   *
   * @param plan Physical Non-Query Plan
   */
  boolean processNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException;
}
