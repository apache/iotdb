package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.junit.Before;
import org.junit.Test;

public class TestIndexStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test (expected = NullPointerException.class)
  // it contains where clause. The where clause will be added into the overall operator, which is null
  public void createIndex1() {
    RootOperator op = generator.getLogicalPlan(
            "create index on root.a.b.c using kvindex with window_length=50 where time > 123");
  }

  @Test (expected = NullPointerException.class)
  public void createIndex2() {
    RootOperator op = generator.getLogicalPlan(
            "create index on root.a.b.c using kv-match2 with xxx=50,xxx=123 where time > now();");
  }

  @Test
  public void dropIndex() {
    RootOperator op = generator.getLogicalPlan("drop index kvindex on root.a.b.c");
  }
}
