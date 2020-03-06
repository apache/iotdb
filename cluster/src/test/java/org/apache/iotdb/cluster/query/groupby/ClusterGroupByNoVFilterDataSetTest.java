package org.apache.iotdb.cluster.query.groupby;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Test;

public class ClusterGroupByNoVFilterDataSetTest extends BaseQueryTest {

  @Test
  public void test() throws StorageEngineException, IOException {
    QueryContext queryContext =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    GroupByPlan groupByPlan = new GroupByPlan();
    List<Path> pathList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<String> aggregations = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      pathList.add(new Path(TestUtils.getTestSeries(i, 0)));
      dataTypes.add(TSDataType.DOUBLE);
      aggregations.add(SQLConstant.COUNT);
    }
    groupByPlan.setPaths(pathList);
    groupByPlan.setDeduplicatedPaths(pathList);
    groupByPlan.setDataTypes(dataTypes);
    groupByPlan.setDeduplicatedDataTypes(dataTypes);
    groupByPlan.setAggregations(aggregations);
    groupByPlan.setDeduplicatedAggregations(aggregations);

    groupByPlan.setStartTime(0);
    groupByPlan.setEndTime(20);
    groupByPlan.setSlidingStep(5);
    groupByPlan.setInterval(5);

    ClusterGroupByNoVFilterDataSet dataSet = new ClusterGroupByNoVFilterDataSet(queryContext,
        groupByPlan, testMetaMember);

    Object[][] answers = new Object[][] {
        new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
        new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
        new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
        new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
    };
    for (Object[] answer : answers) {
      checkDoubleDataset(dataSet, answer);
    }
    assertFalse(dataSet.hasNext());
  }
}
