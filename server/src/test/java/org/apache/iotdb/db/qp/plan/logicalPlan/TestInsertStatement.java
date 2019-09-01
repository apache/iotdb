package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestInsertStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test
  public void multiInsert() {
    RootOperator op = generator.getLogicalPlan(
            "insert into root.vehicle.d0 (timestamp, s0, s1, s2)  values(12345678 , -1011.666, 'da#$%fa', FALSE)");
    assertEquals(SQLConstant.TOK_INSERT, op.getTokenIntType());
    assertEquals(new Path("root.vehicle.d0"),((InsertOperator)op).getSelectedPaths().get(0));
    assertEquals(12345678L, ((InsertOperator)op).getTime());
    String[] expectedMeasurementList = new String[]{"s0", "s1", "s2"};
    assertArrayEquals(expectedMeasurementList, ((InsertOperator)op).getMeasurementList());
    String[] expectedValueList = new String[]{"-1011.666", "'da#$%fa'", "FALSE"};
    assertArrayEquals(expectedValueList, ((InsertOperator)op).getValueList());
  }

  @Test
  public void multiInsert2() {
    RootOperator op = generator.getLogicalPlan("insert into root.vehicle.d0 (timestamp, s0, s1)  values(now() , -1011.666, 1231);");
    assertEquals(SQLConstant.TOK_INSERT, op.getTokenIntType());
    assertEquals(new Path("root.vehicle.d0"),((InsertOperator)op).getSelectedPaths().get(0));
    assertTrue(System.currentTimeMillis() - ((InsertOperator)op).getTime() < 10);
    String[] expectedMeasurementList = new String[]{"s0", "s1"};
    assertArrayEquals(expectedMeasurementList, ((InsertOperator)op).getMeasurementList());
    String[] expectedValueList = new String[]{"-1011.666", "1231"};
    assertArrayEquals(expectedValueList, ((InsertOperator)op).getValueList());
  }

  @Test
  public void multiInsert3() throws LogicalOperatorException {
    RootOperator op = generator.getLogicalPlan("insert into root.vehicle.d0 (timestamp, s0, s1)  values(2016-02-01 11:12:35, -1011.666, 1231)");
    assertEquals(SQLConstant.TOK_INSERT, op.getTokenIntType());
    assertEquals(new Path("root.vehicle.d0"),((InsertOperator)op).getSelectedPaths().get(0));
    assertEquals(parseTimeFormat("2016-02-01 11:12:35"), ((InsertOperator)op).getTime());
    String[] expectedMeasurementList = new String[]{"s0", "s1"};
    assertArrayEquals(expectedMeasurementList, ((InsertOperator)op).getMeasurementList());
    String[] expectedValueList = new String[]{"-1011.666", "1231"};
    assertArrayEquals(expectedValueList, ((InsertOperator)op).getValueList());
  }

  @Test
  public void multiInsert4() throws LogicalOperatorException {
    RootOperator op = generator.getLogicalPlan("insert into root.vehicle.d0 (timestamp, s0, s1) "
            + "values(2016-02-01 11:12:35, \'12\"3a\\'bc\', \"12\\\"3abc\")");
    assertEquals(SQLConstant.TOK_INSERT, op.getTokenIntType());
    assertEquals(new Path("root.vehicle.d0"),((InsertOperator)op).getSelectedPaths().get(0));
    assertEquals(parseTimeFormat("2016-02-01 11:12:35"), ((InsertOperator)op).getTime());
    String[] expectedMeasurementList = new String[]{"s0", "s1"};
    assertArrayEquals(expectedMeasurementList, ((InsertOperator)op).getMeasurementList());
    String[] expectedValueList = new String[]{"\'12\"3a\\'bc\'", "\"12\\\"3abc\""};
    assertArrayEquals(expectedValueList, ((InsertOperator)op).getValueList());
  }

  private long parseTimeFormat(String timestampStr) throws LogicalOperatorException {
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return System.currentTimeMillis();
    }
    return DatetimeUtils.convertDatetimeStrToLong(timestampStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
  }
}
