package org.apache.iotdb.cluster.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.Test;

public class ClusterAggregateExecutorTest extends BaseQueryTest {

  private ClusterAggregateExecutor executor;

  @Test
  public void testNoFilter()
      throws MetadataException, QueryProcessException, StorageEngineException, IOException {
    AggregationPlan plan = new AggregationPlan();
    List<Path> paths = Arrays.asList(
        new Path(TestUtils.getTestSeries(0, 0)),
        new Path(TestUtils.getTestSeries(0, 1)),
        new Path(TestUtils.getTestSeries(0, 2)),
        new Path(TestUtils.getTestSeries(0, 3)),
        new Path(TestUtils.getTestSeries(0, 4)));
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE,
        TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE);
    List<String> aggregations = Arrays.asList(SQLConstant.MIN_TIME, SQLConstant.MAX_VALUE,
        SQLConstant.AVG, SQLConstant.COUNT, SQLConstant.SUM);
    plan.setPaths(paths);
    plan.setDeduplicatedPaths(paths);
    plan.setDataTypes(dataTypes);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setAggregations(aggregations);
    plan.setDeduplicatedAggregations(aggregations);

    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    executor = new ClusterAggregateExecutor(plan, metaGroupMember);
    QueryDataSet queryDataSet = executor.executeWithoutValueFilter(context);
    assertTrue(queryDataSet.hasNext());
    RowRecord record = queryDataSet.next();
    List<Field> fields = record.getFields();
    assertEquals(5, fields.size());
    Object[] answers = new Object[] {0,0,0,0,0};
    for (int i = 0; i < 5; i++) {
      assertEquals(answers[i], fields.get(i));
    }
    assertFalse(queryDataSet.hasNext());
  }

  @Test
  public void testFilter()
      throws MetadataException, QueryProcessException, StorageEngineException, IOException {
    AggregationPlan plan = new AggregationPlan();
    List<Path> paths = Arrays.asList(
        new Path(TestUtils.getTestSeries(0, 0)),
        new Path(TestUtils.getTestSeries(0, 1)),
        new Path(TestUtils.getTestSeries(0, 2)),
        new Path(TestUtils.getTestSeries(0, 3)),
        new Path(TestUtils.getTestSeries(0, 4)));
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE,
        TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE);
    List<String> aggregations = Arrays.asList(SQLConstant.MIN_TIME, SQLConstant.MAX_VALUE,
        SQLConstant.AVG, SQLConstant.COUNT, SQLConstant.SUM);
    plan.setPaths(paths);
    plan.setDeduplicatedPaths(paths);
    plan.setDataTypes(dataTypes);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setAggregations(aggregations);
    plan.setDeduplicatedAggregations(aggregations);
    plan.setExpression(BinaryExpression.and(
        new SingleSeriesExpression(new Path(TestUtils.getTestSeries(0, 0)),
        ValueFilter.ltEq(8.0)),
        new GlobalTimeExpression(TimeFilter.gtEq(3))));

    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    executor = new ClusterAggregateExecutor(plan, metaGroupMember);
    QueryDataSet queryDataSet = executor.executeWithValueFilter(context);
    assertTrue(queryDataSet.hasNext());
    RowRecord record = queryDataSet.next();
    List<Field> fields = record.getFields();
    assertEquals(5, fields.size());
    Object[] answers = new Object[] {0,0,0,0,0};
    for (int i = 0; i < 5; i++) {
      assertEquals(answers[i], fields.get(i));
    }
    assertFalse(queryDataSet.hasNext());
  }
}
