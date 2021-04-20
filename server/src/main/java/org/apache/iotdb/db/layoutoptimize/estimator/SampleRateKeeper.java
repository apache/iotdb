package org.apache.iotdb.db.layoutoptimize.estimator;

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
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class SampleRateKeeper {
  // deviceId -> measurement -> sampleRate
  Map<String, Map<String, Integer>> sampleRateMap;
  Planner processor = new Planner();
  IPlanExecutor executor;
  long defaultQueryRange = 7L * 24L * 60L * 60L * 1000L;
  private static final SampleRateKeeper INSTANCE = new SampleRateKeeper();

  private SampleRateKeeper() {
    try {
      executor = new PlanExecutor();
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }
  }

  public static SampleRateKeeper getInstance() {
    return INSTANCE;
  }

  public int getSampleRate(String deviceId, String measurement) throws SampleRateNoExistsException {
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
    String sql = String.format("select count(*) from %s", deviceId);
    QueryPlan physicalPlan = null;
    try {
      physicalPlan = (QueryPlan) processor.parseSQLToPhysicalPlan(sql);
    } catch (QueryProcessException e) {
      e.printStackTrace();
      return;
    }
    int fetchSize = physicalPlan.getPaths().size();
    long queryId =
        QueryResourceManager.getInstance()
            .assignQueryId(true, fetchSize, physicalPlan.getPaths().size());
    QueryContext context = new QueryContext(queryId, false);
    QueryDataSet dataSet = executor.processQuery(physicalPlan, context);
    dataSet.setFetchSize(fetchSize);
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      System.out.println(rowRecord.toString());
    }
  }

  public void updateSampleRate(String deviceId)
      throws QueryProcessException, TException, StorageEngineException, SQLException, IOException,
          InterruptedException, QueryFilterOptimizationException, MetadataException {
    updateSampleRate(deviceId, defaultQueryRange);
  }
}
