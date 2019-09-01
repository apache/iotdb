package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.UpdateOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.sql.parse.SqlParseException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestUpdateStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test(expected = SqlParseException.class)
  public void updateValueWithTimeFilter1() {
     RootOperator op = generator.getLogicalPlan(
             "UPDATE root.laptop SET d1.s1 = -33000, mac.d1.s2 = 'string' WHERE root.laptop.d1.s2 = TRUE");
  }

  @Test
  public void updateValueWithTimeFilter2() {
    RootOperator op = generator.getLogicalPlan(
            "UPDATE root.laptop SET d1.s1 = -33000 WHERE root.laptop.d1.s2 = TRUE");
    assertEquals(SQLConstant.TOK_UPDATE, op.getTokenIntType());
    Path expectedFromPath = new Path("root.laptop");
    assertEquals(expectedFromPath, ((UpdateOperator)op).getFromOperator().getPrefixPaths().get(0));
    Path expectedSelectPath = new Path("d1.s1");
    assertEquals(expectedSelectPath, ((UpdateOperator)op).getSelectedPaths().get(0));
    assertEquals(SQLConstant.TOK_SELECT, ((UpdateOperator)op).getSelectOperator().getTokenIntType());
    assertEquals("-33000", ((UpdateOperator)op).getValue());
    Path expectWherePath = new Path("root.laptop.d1.s2");
    assertEquals(expectWherePath, ((UpdateOperator)op).getFilterOperator().getSinglePath());
    assertEquals(SQLConstant.EQUAL, ((UpdateOperator)op).getFilterOperator().getTokenIntType());
    assertEquals("TRUE",((BasicFunctionOperator)((UpdateOperator)op).getFilterOperator()).getValue());
  }

  @Test
  public void updateValueWithTimeFilter3() {
    RootOperator op = generator.getLogicalPlan(
            "UPDATE root.laptop SET mac.d1.s2 = 'string' WHERE root.laptop.d1.s2 = TRUE");
    assertEquals(SQLConstant.TOK_UPDATE, op.getTokenIntType());
    Path expectedFromPath = new Path("root.laptop");
    assertEquals(expectedFromPath, ((UpdateOperator)op).getFromOperator().getPrefixPaths().get(0));
    Path expectedSelectPath = new Path("mac.d1.s2");
    assertEquals(expectedSelectPath, ((UpdateOperator)op).getSelectedPaths().get(0));
    assertEquals(SQLConstant.TOK_SELECT, ((UpdateOperator)op).getSelectOperator().getTokenIntType());
    assertEquals("'string'", ((UpdateOperator)op).getValue());
    Path expectWherePath = new Path("root.laptop.d1.s2");
    assertEquals(expectWherePath, ((UpdateOperator)op).getFilterOperator().getSinglePath());
    assertEquals(SQLConstant.EQUAL, ((UpdateOperator)op).getFilterOperator().getTokenIntType());
    assertEquals("TRUE",((BasicFunctionOperator)((UpdateOperator)op).getFilterOperator()).getValue());
  }

  @Test
  public void updateValueWithTimeFilter4() {
    RootOperator op = generator.getLogicalPlan(
            "UPDATE root.laptop SET mac.d1.s2 = FALSE WHERE not(d1.s2 = TRUE)");
    assertEquals(SQLConstant.TOK_UPDATE, op.getTokenIntType());
    Path expectedFromPath = new Path("root.laptop");
    assertEquals(expectedFromPath, ((UpdateOperator)op).getFromOperator().getPrefixPaths().get(0));
    Path expectedSelectPath = new Path("mac.d1.s2");
    assertEquals(expectedSelectPath, ((UpdateOperator)op).getSelectedPaths().get(0));
    assertEquals(SQLConstant.TOK_SELECT, ((UpdateOperator)op).getSelectOperator().getTokenIntType());
    assertEquals("FALSE", ((UpdateOperator)op).getValue());
    assertEquals(SQLConstant.KW_NOT, ((UpdateOperator)op).getFilterOperator().getTokenIntType());
    BasicFunctionOperator bf = (BasicFunctionOperator) ((UpdateOperator)op).getFilterOperator().getChildren().get(0);
    assertEquals(new Path("d1.s2"), bf.getSinglePath());
    assertEquals(SQLConstant.EQUAL, bf.getTokenIntType());
    assertEquals("TRUE",bf.getValue());
  }

  @Test
  public void updateValueWithTimeFilter5() {
    RootOperator op = generator.getLogicalPlan(
            "UPDATE root.laptop SET d1.s1 = -33.54 WHERE not(time <= 1) and  d1.s2 = TRUE;");
    assertEquals(SQLConstant.TOK_UPDATE, op.getTokenIntType());
    Path expectedFromPath = new Path("root.laptop");
    assertEquals(expectedFromPath, ((UpdateOperator)op).getFromOperator().getPrefixPaths().get(0));
    Path expectedSelectPath = new Path("d1.s1");
    assertEquals(expectedSelectPath, ((UpdateOperator)op).getSelectedPaths().get(0));
    assertEquals(SQLConstant.TOK_SELECT, ((UpdateOperator)op).getSelectOperator().getTokenIntType());
    assertEquals("-33.54", ((UpdateOperator)op).getValue());
    assertEquals(SQLConstant.KW_AND, ((UpdateOperator)op).getFilterOperator().getTokenIntType());
    FilterOperator bf1 =  ((UpdateOperator)op).getFilterOperator().getChildren().get(0);
    BasicFunctionOperator bf2 = (BasicFunctionOperator) ((UpdateOperator)op).getFilterOperator().getChildren().get(1);
    BasicFunctionOperator bf3 = (BasicFunctionOperator)bf1.getChildren().get(0);
    assertEquals(new Path("time"), bf3.getSinglePath());
    assertEquals(SQLConstant.LESSTHANOREQUALTO, bf3.getTokenIntType());
    assertEquals("1", bf3.getValue());
    assertEquals(new Path("d1.s2"), bf2.getSinglePath());
    assertEquals(SQLConstant.EQUAL, bf2.getTokenIntType());
    assertEquals("TRUE",bf2.getValue());
  }
}
