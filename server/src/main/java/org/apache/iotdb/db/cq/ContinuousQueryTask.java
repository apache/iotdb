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
package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContinuousQueryTask extends WrappedRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousQueryTask.class);

  private static final Pattern PATH_NODE_NAME_PATTERN = Pattern.compile("\\$\\{\\w+}");
  private static final int EXECUTION_BATCH_SIZE = IoTDBConstant.DEFAULT_FETCH_SIZE;

  private final ServiceProvider serviceProvider;

  // To save the continuous query info
  private final CreateContinuousQueryPlan continuousQueryPlan;
  // Next timestamp to execute a query
  private final long windowEndTimestamp;

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
    final String sql = generateSQL();

    // construct query plan
    final GroupByTimePlan queryPlan =
        (GroupByTimePlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(sql);
    if (queryPlan.getDeduplicatedPaths().isEmpty()) {
      if (continuousQueryPlan.isDebug()) {
        LOGGER.info(continuousQueryPlan.getContinuousQueryName() + ": deduplicated paths empty.");
      }
      return;
    }

    // construct query dataset
    final long queryId = QueryResourceManager.getInstance().assignQueryId(true);
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
      doInsert(queryDataSet, queryPlan);
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
    }
  }

  private String generateSQL() {
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

  private void doInsert(QueryDataSet queryDataSet, GroupByTimePlan queryPlan)
      throws IOException, IllegalPathException, QueryProcessException, StorageGroupNotSetException,
          StorageEngineException {
    int columnSize = queryDataSet.getDataTypes().size();
    TSDataType dataType =
        TypeInferenceUtils.getAggrDataType(
            queryPlan.getAggregations().get(0), queryPlan.getDataTypes().get(0));
    InsertTabletPlan[] insertTabletPlans =
        generateInsertTabletPlans(columnSize, queryDataSet, dataType);

    int batchSize =
        (int)
            Math.min(
                EXECUTION_BATCH_SIZE,
                Math.ceil(
                    (float) continuousQueryPlan.getForInterval()
                        / (continuousQueryPlan.getGroupByTimeInterval())));
    Object[][] columns = constructColumns(columnSize, batchSize, dataType);
    long[][] timestamps = new long[columnSize][batchSize];
    int[] rowNums = new int[columnSize];

    boolean hasNext = true;
    while (hasNext) {
      int rowNum = 0;

      while (++rowNum <= batchSize) {
        if (!queryDataSet.hasNext()) {
          hasNext = false;
          break;
        }
        fillColumns(columns, dataType, queryDataSet.next(), rowNums, timestamps);
      }

      for (int i = 0; i < columnSize; i++) {
        if (rowNums[i] > 0) {
          insertTabletPlans[i].setTimes(timestamps[i]);
          insertTabletPlans[i].setColumns(columns[i]);
          insertTabletPlans[i].setRowCount(rowNums[i]);
          serviceProvider.executeNonQuery(insertTabletPlans[i]);
        }
      }
    }
  }

  private InsertTabletPlan[] generateInsertTabletPlans(
      int columnSize, QueryDataSet result, TSDataType dataType) throws IllegalPathException {
    List<PartialPath> targetPaths = generateTargetPaths(result.getPaths());
    InsertTabletPlan[] insertTabletPlans = new InsertTabletPlan[columnSize];
    String[] measurements = new String[] {targetPaths.get(0).getMeasurement()};
    List<Integer> dataTypes = Collections.singletonList(dataType.ordinal());

    for (int i = 0; i < columnSize; i++) {
      insertTabletPlans[i] =
          new InsertTabletPlan(
              new PartialPath(targetPaths.get(i).getDevice()), measurements, dataTypes);
    }

    return insertTabletPlans;
  }

  private Object[][] constructColumns(int columnSize, int fetchSize, TSDataType dataType) {
    Object[][] columns = new Object[columnSize][1];
    for (int i = 0; i < columnSize; i++) {
      switch (dataType) {
        case DOUBLE:
          columns[i][0] = new double[fetchSize];
          break;
        case INT64:
          columns[i][0] = new long[fetchSize];
          break;
        case INT32:
          columns[i][0] = new int[fetchSize];
          break;
        case FLOAT:
          columns[i][0] = new float[fetchSize];
          break;
        default:
          break;
      }
    }
    return columns;
  }

  private void fillColumns(
      Object[][] columns,
      TSDataType dataType,
      RowRecord record,
      int[] rowNums,
      long[][] timestamps) {
    List<Field> fields = record.getFields();
    long ts = record.getTimestamp();

    for (int i = 0; i < columns.length; i++) {
      Field field = fields.get(i);
      if (field != null) {
        timestamps[i][rowNums[i]] = ts;
        switch (dataType) {
          case DOUBLE:
            ((double[]) columns[i][0])[rowNums[i]] = field.getDoubleV();
            break;
          case INT64:
            ((long[]) columns[i][0])[rowNums[i]] = field.getLongV();
            break;
          case INT32:
            ((int[]) columns[i][0])[rowNums[i]] = field.getIntV();
            break;
          case FLOAT:
            ((float[]) columns[i][0])[rowNums[i]] = field.getFloatV();
            break;
          default:
        }

        rowNums[i]++;
      }
    }
  }

  private List<PartialPath> generateTargetPaths(List<Path> rawPaths) throws IllegalPathException {
    List<PartialPath> targetPaths = new ArrayList<>(rawPaths.size());
    for (Path rawPath : rawPaths) {
      targetPaths.add(new PartialPath(fillTargetPathTemplate((PartialPath) rawPath)));
    }
    return targetPaths;
  }

  private String fillTargetPathTemplate(PartialPath rawPath) {
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
