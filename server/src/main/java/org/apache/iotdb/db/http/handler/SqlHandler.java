package org.apache.iotdb.db.http.handler;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.cost.statistic.Operation;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.control.TracingManager;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlHandler extends Handler {
  private static final int DEFAULT_FETCH_SIZE = 10000;
  private boolean ignoreTimeStamp = false;
  private static final Logger SLOW_SQL_LOGGER = LoggerFactory.getLogger("SLOW_SQL");

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public JsonElement executeStatement(String sql, int fetchSize, ZoneId zoneId)
      throws QueryProcessException, AuthException, StorageGroupNotSetException, StorageEngineException {
    checkLogin();
    PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan(sql, zoneId, fetchSize);
    if (physicalPlan.isQuery()) {
      return internalExecuteQueryStatement(sql, physicalPlan, fetchSize);
    } else {
      if(executeNonQuery(physicalPlan)) {
          return getSuccessfulObject();
      } else {
        throw new QueryProcessException(sql + "executed unsuccessfully");
      }
    }
  }

  /**
   * @param plan must be a plan for Query: FillQueryPlan, AggregationPlan, GroupByTimePlan, some
   *             AuthorPlan
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private JsonArray internalExecuteQueryStatement(String statement, PhysicalPlan plan, int fetchSize)
      throws QueryProcessException {

    long startTime = System.currentTimeMillis();
    long queryId = -1;
    try {

      // In case users forget to set this field in query, use the default value
      if (fetchSize == 0) {
        fetchSize = DEFAULT_FETCH_SIZE;
      }

      if (plan instanceof ShowTimeSeriesPlan) {
        //If the user does not pass the limit, then set limit = fetchSize and haslimit=false,else set haslimit = true
        if (((ShowTimeSeriesPlan) plan).getLimit() == 0) {
          ((ShowTimeSeriesPlan) plan).setLimit(fetchSize);
          ((ShowTimeSeriesPlan) plan).setHasLimit(false);
        } else {
          ((ShowTimeSeriesPlan) plan).setHasLimit(true);
        }
      }
      if (plan instanceof QueryPlan && !((QueryPlan) plan).isAlignByTime()) {
        if (plan.getOperatorType() == OperatorType.AGGREGATION) {
          throw new QueryProcessException("Aggregation doesn't support disable align clause.");
        }
        if (plan.getOperatorType() == OperatorType.FILL) {
          throw new QueryProcessException("Fill doesn't support disable align clause.");
        }
        if (plan.getOperatorType() == OperatorType.GROUPBYTIME) {
          throw new QueryProcessException("Group by doesn't support disable align clause.");
        }
      }
      if (plan.getOperatorType() == OperatorType.AGGREGATION) {
        // the actual row number of aggregation query is 1
        fetchSize = 1;
      }

      if (plan instanceof GroupByTimePlan) {
        GroupByTimePlan groupByTimePlan = (GroupByTimePlan) plan;
        // the actual row number of group by query should be calculated from startTime, endTime and interval.
        fetchSize = Math.min(
            (int) ((groupByTimePlan.getEndTime() - groupByTimePlan.getStartTime()) / groupByTimePlan
                .getInterval()), fetchSize);
      }

      // get deduplicated path num
      int deduplicatedPathNum = -1;
      if (plan instanceof AlignByDevicePlan) {
        deduplicatedPathNum = ((AlignByDevicePlan) plan).getMeasurements().size();
      } else if (plan instanceof LastQueryPlan) {
        // dataset of last query consists of three column: time column + value column = 1 deduplicatedPathNum
        // and we assume that the memory which sensor name takes equals to 1 deduplicatedPathNum
        deduplicatedPathNum = 2;
        // last query's actual row number should be the minimum between the number of series and fetchSize
        fetchSize = Math.min(((LastQueryPlan) plan).getDeduplicatedPaths().size(), fetchSize);
      } else if (plan instanceof RawDataQueryPlan) {
        deduplicatedPathNum = ((RawDataQueryPlan) plan).getDeduplicatedPaths().size();
      }

      // generate the queryId for the operation
      queryId = generateQueryId(fetchSize, deduplicatedPathNum);
      if (plan instanceof QueryPlan && config.isEnablePerformanceTracing()) {
        if (!(plan instanceof AlignByDevicePlan)) {
          TracingManager.getInstance()
              .writeQueryInfo(queryId, statement, startTime, plan.getPaths().size());
        } else {
          TracingManager.getInstance().writeQueryInfo(queryId, statement, startTime);
        }
      }

      if (plan instanceof AuthorPlan) {
        plan.setLoginUserName(username);
      }

      JsonArray jsonArray = null;
      // execute it before createDataSet since it may change the content of query plan
      if (plan instanceof QueryPlan) {
        jsonArray = getQueryColumnHeaders(plan, username);
      }
      // create and cache dataset
      QueryDataSet newDataSet = createQueryDataSet(queryId, plan);

      if (plan instanceof ShowPlan || plan instanceof AuthorPlan) {
        jsonArray = getListDataSetHeaders(newDataSet);
      }

      if (plan.getOperatorType() == OperatorType.AGGREGATION) {
        ignoreTimeStamp = true;
      } // else default ignoreTimeStamp is false

      while(newDataSet.hasNext()) {
        RowRecord record = newDataSet.next();
        JsonArray row = new JsonArray();
        if(!ignoreTimeStamp) {
          row.add(record.getTimestamp());
        }
        for(Field field : record.getFields()) {
          switch (field.getDataType()) {
            case INT32:
              row.add(field.getIntV());
              break;
            case INT64:
              row.add(field.getLongV());
              break;
            case FLOAT:
              row.add(field.getFloatV());
              break;
            case DOUBLE:
              row.add(field.getDoubleV());
              break;
            case TEXT:
              row.add(field.getBinaryV().toString());
              break;
            case BOOLEAN:
              row.add(field.getBoolV());
              break;
          }
        }
        if (jsonArray != null) {
          jsonArray.add(row);
        }
      }

      if (plan instanceof AlignByDevicePlan && config.isEnablePerformanceTracing()) {
        TracingManager.getInstance()
            .writePathsNum(queryId, ((AlignByDeviceDataSet) newDataSet).getPathsNum());
      }

      return jsonArray;
    } catch (Exception e) {
      logger.warn("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      if (e instanceof NullPointerException) {
        e.printStackTrace();
      }
      if (queryId != -1) {
        try {
          releaseQueryResource(queryId);
        } catch (StorageEngineException ex) {
          logger.warn("Error happened while releasing query resource: ", ex);
        }
      }
      Throwable cause = e;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      throw new QueryProcessException(cause.getMessage());
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_QUERY, startTime);
      long costTime = System.currentTimeMillis() - startTime;
      if (costTime >= config.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, statement);
      }
      if (config.isDebugOn()) {
        SLOW_SQL_LOGGER.info(
            "ChunkCache used memory proportion: {}\nChunkMetadataCache used memory proportion: {}\n"
                + "TimeSeriesMetadataCache used memory proportion: {}",
            ChunkCache.getInstance()
                .getUsedMemoryProportion(),
            ChunkMetadataCache.getInstance().getUsedMemoryProportion(), TimeSeriesMetadataCache
                .getInstance().getUsedMemoryProportion());
      }
    }
  }

  /**
   * get ResultSet schema
   */
  private JsonArray getQueryColumnHeaders(PhysicalPlan physicalPlan, String username)
      throws AuthException, TException, QueryProcessException, MetadataException {

    List<String> respColumns = new ArrayList<>();
    List<String> columnsTypes = new ArrayList<>();

    // check permissions
    if (!checkAuthorization(physicalPlan.getPaths(), physicalPlan, username)) {
      throw new AuthException("No permissions for this operation " + physicalPlan.getOperatorType());
    }


    // align by device query
    QueryPlan plan = (QueryPlan) physicalPlan;
    if (plan instanceof AlignByDevicePlan) {
      getAlignByDeviceQueryHeaders((AlignByDevicePlan) plan, respColumns, columnsTypes);
    } else if (plan instanceof LastQueryPlan) {
      // Last Query should return different respond instead of the static one
      // because the query dataset and query id is different although the header of last query is same.
      JsonArray jsonArray = new JsonArray();
      JsonArray header = new JsonArray();
      header.add(COLUMN_TIMESERIES);
      header.add(COLUMN_VALUE);
      jsonArray.add(header);
      return jsonArray;
    } else if (plan instanceof AggregationPlan && ((AggregationPlan) plan).getLevel() >= 0) {
      Map<Integer, String> pathIndex = new HashMap<>();
      Map<String, AggregateResult> finalPaths = FilePathUtils
          .getPathByLevel((AggregationPlan) plan, pathIndex);
      for (Map.Entry<String, AggregateResult> entry : finalPaths.entrySet()) {
        respColumns
            .add(entry.getValue().getAggregationType().toString() + "(" + entry.getKey() + ")");
        columnsTypes.add(entry.getValue().getResultDataType().toString());
      }
    } else {
      getWideQueryHeaders(plan, respColumns, columnsTypes);
    }
    JsonArray jsonArray = new JsonArray();
    JsonArray header = new JsonArray();
    for(String columns : respColumns) {
      header.add(columns);
    }
    jsonArray.add(header);
    return jsonArray;
  }

  // wide means not align by device
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void getWideQueryHeaders(
      QueryPlan plan, List<String> respColumns, List<String> columnTypes)
      throws TException, MetadataException {
    // Restore column header of aggregate to func(column_name), only
    // support single aggregate function for now
    List<PartialPath> paths = plan.getPaths();
    List<TSDataType> seriesTypes = new ArrayList<>();
    switch (plan.getOperatorType()) {
      case QUERY:
      case FILL:
        for (PartialPath path : paths) {
          String column;
          if (path.isTsAliasExists()) {
            column = path.getTsAlias();
          } else {
            column = path.isMeasurementAliasExists() ? path.getFullPathWithAlias()
                : path.getFullPath();
          }
          respColumns.add(column);
          seriesTypes.add(getSeriesTypeByPath(path));
        }
        break;
      case AGGREGATION:
      case GROUPBYTIME:
      case GROUP_BY_FILL:
        List<String> aggregations = plan.getAggregations();
        if (aggregations.size() != paths.size()) {
          for (int i = 1; i < paths.size(); i++) {
            aggregations.add(aggregations.get(0));
          }
        }
        for (int i = 0; i < paths.size(); i++) {
          PartialPath path = paths.get(i);
          String column;
          if (path.isTsAliasExists()) {
            column = path.getTsAlias();
          } else {
            column = path.isMeasurementAliasExists()
                ? aggregations.get(i) + "(" + paths.get(i).getFullPathWithAlias() + ")"
                : aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")";
          }
          respColumns.add(column);
        }
        seriesTypes = getSeriesTypesByPaths(paths, aggregations);
        break;
      default:
        throw new TException("unsupported query type: " + plan.getOperatorType());
    }

    for (TSDataType seriesType : seriesTypes) {
      columnTypes.add(seriesType.toString());
    }
  }

  private void getAlignByDeviceQueryHeaders(
      AlignByDevicePlan plan, List<String> respColumns, List<String> columnTypes) {
    // set columns in TSExecuteStatementResp.
    respColumns.add(SQLConstant.ALIGNBY_DEVICE_COLUMN_NAME);

    // get column types and do deduplication
    columnTypes.add(TSDataType.TEXT.toString()); // the DEVICE column of ALIGN_BY_DEVICE result
    List<TSDataType> deduplicatedColumnsType = new ArrayList<>();
    deduplicatedColumnsType.add(TSDataType.TEXT); // the DEVICE column of ALIGN_BY_DEVICE result

    Set<String> deduplicatedMeasurements = new LinkedHashSet<>();
    Map<String, TSDataType> measurementDataTypeMap = plan.getColumnDataTypeMap();

    // build column header with constant and non exist column and deduplication
    List<String> measurements = plan.getMeasurements();
    Map<String, String> measurementAliasMap = plan.getMeasurementAliasMap();
    Map<String, MeasurementType> measurementTypeMap = plan.getMeasurementTypeMap();
    for (String measurement : measurements) {
      TSDataType type = TSDataType.TEXT;
      switch (measurementTypeMap.get(measurement)) {
        case Exist:
          type = measurementDataTypeMap.get(measurement);
          break;
        case NonExist:
        case Constant:
          break;
      }
      respColumns.add(measurementAliasMap.getOrDefault(measurement, measurement));
      columnTypes.add(type.toString());

      if (!deduplicatedMeasurements.contains(measurement)) {
        deduplicatedMeasurements.add(measurement);
        deduplicatedColumnsType.add(type);
      }
    }

    // save deduplicated measurementColumn names and types in QueryPlan for the next stage to use.
    // i.e., used by AlignByDeviceDataSet constructor in `fetchResults` stage.
    plan.setMeasurements(new ArrayList<>(deduplicatedMeasurements));
    plan.setDataTypes(deduplicatedColumnsType);

    // set these null since they are never used henceforth in ALIGN_BY_DEVICE query processing.
    plan.setPaths(null);
  }

  protected List<TSDataType> getSeriesTypesByPaths(List<PartialPath> paths,
      List<String> aggregations)
      throws MetadataException {
    return SchemaUtils.getSeriesTypesByPaths(paths, aggregations);
  }

  protected TSDataType getSeriesTypeByPath(PartialPath path) throws MetadataException {
    return SchemaUtils.getSeriesTypeByPaths(path);
  }

  private long generateQueryId(int fetchSize, int deduplicatedPathNum) {
    return QueryResourceManager.getInstance()
        .assignQueryId(true, fetchSize, deduplicatedPathNum);
  }

  private boolean checkAuthorization(List<PartialPath> paths, PhysicalPlan plan, String username)
      throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username, paths, plan.getOperatorType(), targetUser);
  }

  /**
   * create QueryDataSet and buffer it for fetchResults
   */
  private QueryDataSet createQueryDataSet(long queryId, PhysicalPlan physicalPlan)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException,
      IOException, MetadataException, SQLException, TException, InterruptedException {

    QueryContext context = genQueryContext(queryId);
    return executor.processQuery(physicalPlan, context);
  }

  protected QueryContext genQueryContext(long queryId) {
    return new QueryContext(queryId);
  }

  private JsonArray getListDataSetHeaders(QueryDataSet dataSet) {
    JsonArray header = new JsonArray();
    for(Path path : dataSet.getPaths()) {
      header.add(path.getFullPath());
    }
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(header);
    return jsonArray;
  }

  /**
   * release single operation resource
   */
  protected void releaseQueryResource(long queryId) throws StorageEngineException {
    // remove the corresponding Physical Plan
    QueryResourceManager.getInstance().endQuery(queryId);
  }

  private boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return executor.processNonQuery(plan);
  }
}
