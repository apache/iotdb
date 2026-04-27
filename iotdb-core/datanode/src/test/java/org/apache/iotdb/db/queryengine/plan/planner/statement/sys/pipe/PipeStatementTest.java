/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner.statement.sys.pipe;

import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StopPipeStatement;

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
    statement.setSourceAttributes(extractorAttributes);
    statement.setProcessorAttributes(processorAttributes);
    statement.setSinkAttributes(connectorAttributes);

    Assert.assertEquals("test", statement.getPipeName());
    Assert.assertEquals(extractorAttributes, statement.getSourceAttributes());
    Assert.assertEquals(processorAttributes, statement.getProcessorAttributes());
    Assert.assertEquals(connectorAttributes, statement.getSinkAttributes());

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
