package org.apache.iotdb.db.queryengine.plan.plan.statement.sys.pipe;

import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.pipe.*;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeStatementTest {
  @Test
  public void testCreatePipeStatement() {
    Map<String, String> extractorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();
    CreatePipeStatement statement = new CreatePipeStatement(StatementType.CREATE_PIPE);

    statement.setPipeName("test");
    statement.setExtractorAttributes(extractorAttributes);
    statement.setProcessorAttributes(processorAttributes);
    statement.setConnectorAttributes(connectorAttributes);

    Assert.assertEquals("test", statement.getPipeName());
    Assert.assertEquals(extractorAttributes, statement.getExtractorAttributes());
    Assert.assertEquals(processorAttributes, statement.getProcessorAttributes());
    Assert.assertEquals(connectorAttributes, statement.getConnectorAttributes());

    Assert.assertEquals(QueryType.WRITE, statement.getQueryType());
  }

  @Test
  public void testDropPipeStatement() {
    DropPipeStatement statement = new DropPipeStatement(StatementType.DROP_PIPE);
    statement.setPipeName("test");
    Assert.assertEquals("test", statement.getPipeName());
    Assert.assertEquals(QueryType.WRITE, statement.getQueryType());
  }

  @Test
  public void testShowPipesStatement() {
    ShowPipesStatement statement = new ShowPipesStatement();
    statement.setPipeName("test");
    statement.setWhereClause(true);
    Assert.assertEquals("test", statement.getPipeName());
    Assert.assertTrue(statement.getWhereClause());
    Assert.assertEquals(QueryType.READ, statement.getQueryType());
  }

  @Test
  public void testStartPipeStatement() {
    StartPipeStatement statement = new StartPipeStatement(StatementType.START_PIPE);
    statement.setPipeName("test");
    Assert.assertEquals("test", statement.getPipeName());
    Assert.assertEquals(QueryType.WRITE, statement.getQueryType());
  }

  @Test
  public void testStopPipeStatement() {
    StopPipeStatement statement = new StopPipeStatement(StatementType.STOP_PIPE);
    statement.setPipeName("test");
    Assert.assertEquals("test", statement.getPipeName());
    Assert.assertEquals(QueryType.WRITE, statement.getQueryType());
  }
}
