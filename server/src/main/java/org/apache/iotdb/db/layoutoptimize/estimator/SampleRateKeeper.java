package org.apache.iotdb.db.layoutoptimize.estimator;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.layoutoptimize.SampleRateNoExistsException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleRateKeeper {
  // deviceId -> measurement -> sampleRate
  Map<String, Map<String, Double>> sampleRateMap = new HashMap<>();
  QueryExecutor executor = new QueryExecutor();
  long defaultQueryRange = 7L * 24L * 60L * 60L * 1000L;
  int queryFetchSize = 20;
  private static final SampleRateKeeper INSTANCE = new SampleRateKeeper();

  private SampleRateKeeper() {}

  public static SampleRateKeeper getInstance() {
    return INSTANCE;
  }

  public double getSampleRate(String deviceId, String measurement)
      throws SampleRateNoExistsException {
    if (!sampleRateMap.containsKey(deviceId)
        || !sampleRateMap.get(deviceId).containsKey(measurement)) {
      throw new SampleRateNoExistsException(
          String.format(
              "the sample rate of %s.%s does not exist in SampleRateKeeper",
              deviceId, measurement));
    }
    return sampleRateMap.get(deviceId).get(measurement);
  }

  public void updateSampleRate(String deviceId, long queryRange)
      throws QueryProcessException, TException, StorageEngineException, SQLException, IOException,
          InterruptedException, QueryFilterOptimizationException, MetadataException {
    String maxTimeSql = String.format("select max_time(*) from %s", deviceId);
    QueryDataSet maxTimeDataSet = executor.executeQuery(maxTimeSql);
    long[] maxTimeForMeasurement = new long[maxTimeDataSet.getPaths().size()];
    long[] minTimeForMeasurement = new long[maxTimeDataSet.getPaths().size()];
    long[] queryRangeForMeasurement = new long[maxTimeDataSet.getPaths().size()];
    int i = 0;
    while (maxTimeDataSet.hasNext()) {
      RowRecord rowRecord = maxTimeDataSet.next();
      List<Field> timeField = rowRecord.getFields();
      for (Field field : timeField) {
        maxTimeForMeasurement[i] = field == null ? -1L : field.getLongV();
        i++;
      }
    }
    String minTimeSql = String.format("select min_time(*) from %s", deviceId);
    QueryDataSet minTimeDataSet = executor.executeQuery(minTimeSql);
    i = 0;
    while (minTimeDataSet.hasNext()) {
      RowRecord rowRecord = minTimeDataSet.next();
      List<Field> fields = rowRecord.getFields();
      for (Field field : fields) {
        minTimeForMeasurement[i] = field == null ? -1L : field.getLongV();
        if (minTimeForMeasurement[i] != -1L && maxTimeForMeasurement[i] != -1L) {
          queryRangeForMeasurement[i] =
              Math.min(queryRange, maxTimeForMeasurement[i] - minTimeForMeasurement[i]);
        } else {
          queryRangeForMeasurement[i] = -1L;
        }
        i++;
      }
    }
    String queryCountSqlPattern = "select count(%s) from %s where time>=%d";
    List<Path> paths = maxTimeDataSet.getPaths();
    for (i = 0; i < maxTimeForMeasurement.length; ++i) {
      if (queryRangeForMeasurement[i] == -1L) continue;
      String sql =
          String.format(
              queryCountSqlPattern,
              paths.get(i).getMeasurement(),
              deviceId,
              maxTimeForMeasurement[i] - queryRangeForMeasurement[i]);
      QueryDataSet cntDataset = executor.executeQuery(sql);
      if (!cntDataset.hasNext()) continue;
      long dataPointNum = cntDataset.next().getFields().get(0).getLongV();
      if (!sampleRateMap.containsKey(deviceId)) {
        sampleRateMap.put(deviceId, new HashMap<>());
      }
      sampleRateMap
          .get(deviceId)
          .put(paths.get(i).getMeasurement(), (double) dataPointNum / queryRangeForMeasurement[i]);
    }
  }

  public void updateSampleRate(String deviceId)
      throws QueryProcessException, TException, StorageEngineException, SQLException, IOException,
          InterruptedException, QueryFilterOptimizationException, MetadataException {
    updateSampleRate(deviceId, defaultQueryRange);
  }

  public boolean hasSampleRateForDevice(String device) {
    return sampleRateMap.containsKey(device);
  }

  private class QueryExecutor {
    Planner processor = new Planner();
    IPlanExecutor executor;

    public QueryExecutor() {
      try {
        executor = new PlanExecutor();
      } catch (QueryProcessException e) {
        e.printStackTrace();
        executor = null;
      }
    }

    public QueryDataSet executeQuery(String sql)
        throws QueryProcessException, TException, StorageEngineException, SQLException, IOException,
            InterruptedException, QueryFilterOptimizationException, MetadataException {
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      config.setMaxQueryDeduplicatedPathNum(10000);
      QueryPlan physicalPlan = null;
      try {
        physicalPlan = (QueryPlan) processor.parseSQLToPhysicalPlan(sql);
      } catch (QueryProcessException e) {
        e.printStackTrace();
        return null;
      }
      long queryId =
          QueryResourceManager.getInstance()
              .assignQueryId(true, queryFetchSize, physicalPlan.getPaths().size());
      QueryContext context = new QueryContext(queryId, false);
      QueryDataSet dataSet = executor.processQuery(physicalPlan, context);
      dataSet.setFetchSize(queryFetchSize);
      return dataSet;
    }
  }
}
