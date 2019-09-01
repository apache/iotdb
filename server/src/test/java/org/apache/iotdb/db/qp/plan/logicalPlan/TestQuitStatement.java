package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.sql.parse.SqlParseException;
import org.junit.Before;
import org.junit.Test;

public class TestQuitStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test (expected = SqlParseException.class)
  public void quit() {
    RootOperator op = generator.getLogicalPlan("quit;");
  }
}
