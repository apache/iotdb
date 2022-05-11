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
package org.apache.iotdb.db.engine.cq;

import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.selectinto.InsertTabletPlansIterator;
import org.apache.iotdb.db.exception.ContinuousQueryException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContinuousQueryTask extends WrappedRunnable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ContinuousQueryTask.class);

  protected static final Pattern PATH_NODE_NAME_PATTERN = Pattern.compile("\\$\\{\\w+}");
  protected static final int EXECUTION_BATCH_SIZE = IoTDBConstant.DEFAULT_FETCH_SIZE;

  protected final ServiceProvider serviceProvider;

  // To save the continuous query info
  protected final CreateContinuousQueryPlan continuousQueryPlan;
  // Next timestamp to execute a query
  protected final long windowEndTimestamp;

  public ContinuousQueryTask(
      CreateContinuousQueryPlan continuousQueryPlan, long windowEndTimestamp) {
    this.continuousQueryPlan = continuousQueryPlan;
    this.windowEndTimestamp = windowEndTimestamp;
    serviceProvider = IoTDB.serviceProvider;
  }

  @Override
  public void runMayThrow()
      throws QueryProcessException, StorageEngineException, IOException, InterruptedException,
          QueryFilterOptimizationException, MetadataException, TException, SQLException {
    // construct logical operator
    final String sql = generateSQL();
    Operator operator = LogicalGenerator.generate(sql, ZoneId.systemDefault());
    if (!operator.isQuery()) {
      throw new ContinuousQueryException(
          String.format("unsupported operation in cq task: %s", operator.getType().name()));
    }
    QueryOperator queryOperator = (QueryOperator) operator;

    // construct query plan
    final GroupByTimePlan queryPlan =
        (GroupByTimePlan) serviceProvider.getPlanner().operatorToPhysicalPlan(queryOperator);
    if (queryPlan.getDeduplicatedPaths().isEmpty()) {
      if (continuousQueryPlan.isDebug()) {
        LOGGER.info(continuousQueryPlan.getContinuousQueryName() + ": deduplicated paths empty.");
      }
      return;
    }

    // construct query dataset
    final long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      final QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              queryPlan.isDebug(),
              System.currentTimeMillis(),
              sql,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      final QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(queryContext, queryPlan, EXECUTION_BATCH_SIZE);
      if (queryDataSet == null || queryDataSet.getPaths().size() == 0) {
        if (continuousQueryPlan.isDebug()) {
          LOGGER.info(continuousQueryPlan.getContinuousQueryName() + ": query result empty.");
        }
        return;
      }

      // insert data into target timeseries
      doInsert(sql, queryOperator, queryPlan, queryDataSet);
    } finally {
      ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
    }
  }

  protected String generateSQL() {
    return continuousQueryPlan.getQuerySqlBeforeGroupByClause()
        + "group by (["
        + (windowEndTimestamp - continuousQueryPlan.getForInterval())
        + ','
        + windowEndTimestamp
        + "),"
        + continuousQueryPlan.getGroupByTimeIntervalString()
        + ") "
        + (continuousQueryPlan.getQuerySqlAfterGroupByClause().equals("") ? "" : ", ")
        + continuousQueryPlan.getQuerySqlAfterGroupByClause();
  }

  protected void doInsert(
      String sql, QueryOperator queryOperator, GroupByTimePlan queryPlan, QueryDataSet queryDataSet)
      throws MetadataException, QueryProcessException, StorageEngineException, IOException {
    InsertTabletPlansIterator insertTabletPlansIterator =
        new InsertTabletPlansIterator(
            queryPlan,
            queryDataSet,
            queryOperator.getFromComponent().getPrefixPaths().get(0),
            generateTargetPaths(queryDataSet.getPaths()),
            false);
    while (insertTabletPlansIterator.hasNext()) {
      List<InsertTabletPlan> insertTabletPlans = insertTabletPlansIterator.next();
      if (insertTabletPlans.isEmpty()) {
        continue;
      }
      if (!serviceProvider.executeNonQuery(new InsertMultiTabletPlan(insertTabletPlans))) {
        throw new ContinuousQueryException(
            String.format(
                "failed to execute cq task %s, sql: %s",
                continuousQueryPlan.getContinuousQueryName(), sql));
      }
    }
  }

  protected List<PartialPath> generateTargetPaths(List<Path> rawPaths) throws IllegalPathException {
    List<PartialPath> targetPaths = new ArrayList<>(rawPaths.size());
    for (Path rawPath : rawPaths) {
      targetPaths.add(new PartialPath(fillTargetPathTemplate((PartialPath) rawPath)));
    }
    return targetPaths;
  }

  protected String fillTargetPathTemplate(PartialPath rawPath) {
    String[] nodes = rawPath.getNodes();
    int indexOfLeftBracket = nodes[0].indexOf("(");
    if (indexOfLeftBracket != -1) {
      nodes[0] = nodes[0].substring(indexOfLeftBracket + 1);
    }
    int indexOfRightBracket = nodes[nodes.length - 1].indexOf(")");
    if (indexOfRightBracket != -1) {
      nodes[nodes.length - 1] = nodes[nodes.length - 1].substring(0, indexOfRightBracket);
    }
    StringBuffer sb = new StringBuffer();
    Matcher m =
        PATH_NODE_NAME_PATTERN.matcher(this.continuousQueryPlan.getTargetPath().getFullPath());
    while (m.find()) {
      String param = m.group();
      String value = nodes[Integer.parseInt(param.substring(2, param.length() - 1).trim())];
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  public void onRejection() {
    LOGGER.warn(
        "continuous query task {} was rejected, sql: {}",
        continuousQueryPlan.getContinuousQueryName(),
        generateSQL());
  }
}
