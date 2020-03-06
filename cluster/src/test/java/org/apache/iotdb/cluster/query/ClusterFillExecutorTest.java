package org.apache.iotdb.cluster.query;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.query.fill.LinearFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.Test;

public class ClusterFillExecutorTest extends BaseQueryTest {

  @Test
  public void testPreviousFill() throws QueryProcessException, StorageEngineException, IOException {
    FillQueryPlan plan = new FillQueryPlan();
    plan.setDeduplicatedPaths(Collections.singletonList(
        new Path(TestUtils.getTestSeries(0, 10))));
    plan.setDeduplicatedDataTypes(Collections.singletonList(TSDataType.DOUBLE));
    plan.setPaths(plan.getDeduplicatedPaths());
    plan.setDataTypes(plan.getDeduplicatedDataTypes());
    long defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    Map<TSDataType, IFill> tsDataTypeIFillMap = Collections.singletonMap(TSDataType.DOUBLE,
        new PreviousFill(TSDataType.DOUBLE, 0, defaultFillInterval));
    plan.setFillType(tsDataTypeIFillMap);
    QueryContext context = new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    ClusterFillExecutor fillExecutor;
    QueryDataSet queryDataSet;
    long[] queryTimes = new long[] {-1, 0, 5, 10, 20};
    Object[][] answers = new Object[][]{
        new Object[]{null},
        new Object[]{0.0},
        new Object[]{0.0},
        new Object[]{10.0},
        new Object[]{10.0},
    };
    for (int i = 0; i < queryTimes.length; i++) {
      fillExecutor = new ClusterFillExecutor(plan.getDeduplicatedPaths(),
          plan.getDeduplicatedDataTypes(), queryTimes[i], plan.getFillType(), testMetaMember);
      queryDataSet = fillExecutor.execute(context);
      checkDoubleDataset(queryDataSet, answers[i]);
      assertFalse(queryDataSet.hasNext());
    }
  }

  @Test
  public void testLinearFill() throws QueryProcessException, StorageEngineException, IOException {
    FillQueryPlan plan = new FillQueryPlan();
    plan.setDeduplicatedPaths(Collections.singletonList(
        new Path(TestUtils.getTestSeries(0, 10))));
    plan.setDeduplicatedDataTypes(Collections.singletonList(TSDataType.DOUBLE));
    plan.setPaths(plan.getDeduplicatedPaths());
    plan.setDataTypes(plan.getDeduplicatedDataTypes());
    long defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    Map<TSDataType, IFill> tsDataTypeIFillMap = Collections.singletonMap(TSDataType.DOUBLE,
        new LinearFill(TSDataType.DOUBLE, 0, defaultFillInterval, defaultFillInterval));
    plan.setFillType(tsDataTypeIFillMap);
    QueryContext context = new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    ClusterFillExecutor fillExecutor;
    QueryDataSet queryDataSet;
    long[] queryTimes = new long[] {-1, 0, 5, 10, 20};
    Object[][] answers = new Object[][]{
        new Object[]{null},
        new Object[]{0.0},
        new Object[]{5.0},
        new Object[]{10.0},
        new Object[]{null},
    };
    for (int i = 0; i < queryTimes.length; i++) {
      fillExecutor = new ClusterFillExecutor(plan.getDeduplicatedPaths(),
          plan.getDeduplicatedDataTypes(), queryTimes[i], plan.getFillType(), testMetaMember);
      queryDataSet = fillExecutor.execute(context);
      checkDoubleDataset(queryDataSet, answers[i]);
      assertFalse(queryDataSet.hasNext());
    }
  }
}
