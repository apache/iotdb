/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.utils.cte.CteDataStore;
import org.apache.iotdb.db.utils.cte.DiskSpiller;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.read.common.block.TsBlock;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CteMaterializer {

  private static final Coordinator coordinator = Coordinator.getInstance();

  private CteMaterializer() {}

  public static void materializeCTE(Analysis analysis, MPPQueryContext context) {
    Set<Query> materializedQueries = new HashSet<>();
    analysis
        .getNamedQueries()
        .forEach(
            (tableRef, query) -> {
              if (query.isMaterialized() && !materializedQueries.contains(query)) {
                if (!context.mayHaveTmpFile()) {
                  context.setMayHaveTmpFile(true);
                }

                String cteName = tableRef.getNode().getName().toString();
                CteDataStore dataStore = fetchCteQueryResult(cteName, query, context);
                if (dataStore != null) {
                  context.addCteDataStore(cteName, dataStore);
                  context.reserveMemoryForFrontEnd(dataStore.getCachedBytes());
                  materializedQueries.add(query);
                }
              }
            });
  }

  public static void cleanUpCTE(Analysis analysis, MPPQueryContext context) {
    Map<String, CteDataStore> cteDataStores = context.getCteDataStores();
    cteDataStores
        .values()
        .forEach(
            dataStore -> {
              context.releaseMemoryReservedForFrontEnd(dataStore.getCachedBytes());
              dataStore.clear();
            });
    cteDataStores.clear();
  }

  public static CteDataStore fetchCteQueryResult(
      String cteName, Query query, MPPQueryContext context) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;
    try {
      final ExecutionResult executionResult =
          coordinator.executeForTableModel(
              query,
              new SqlParser(),
              SessionManager.getInstance().getCurrSession(),
              queryId,
              SessionManager.getInstance()
                  .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
              "Materialize common table expression",
              LocalExecutionPlanner.getInstance().metadata,
              context.getTimeOut(),
              false);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return null;
      }

      String folderPath =
          IoTDBDescriptor.getInstance().getConfig().getCteTmpDir()
              + File.separator
              + context.getQueryId()
              + File.separator;
      CteDataStore cteDataStore =
          new CteDataStore(new DiskSpiller(folderPath, folderPath + cteName));
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          t = e;
          throw new IoTDBRuntimeException(
              String.format("Fail to materialize CTE because %s", e.getMessage()),
              e.getErrorCode(),
              e.isUserException());
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          continue;
        }
        cteDataStore.addTsBlock(tsBlock.get());
      }
      return cteDataStore;
    } catch (final Throwable throwable) {
      t = throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return null;
  }
}
