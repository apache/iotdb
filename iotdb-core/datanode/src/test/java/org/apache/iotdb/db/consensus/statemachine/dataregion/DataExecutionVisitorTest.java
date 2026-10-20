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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.rpc.TSStatusCode;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;

public class DataExecutionVisitorTest {

  @Test
  public void testInsertRowsFailureKeepsExceptionInLog() throws Exception {
    final DataRegion dataRegion = Mockito.mock(DataRegion.class);
    final SemanticException failure = new SemanticException("injected write failure");
    Mockito.doThrow(failure).when(dataRegion).insert(any(InsertRowsNode.class));

    final InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("write-log-test"),
            new PartialPath("root.sg.device"),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT64},
            1L,
            new Object[] {1L},
            false);
    final InsertRowsNode insertRowsNode =
        new InsertRowsNode(
            new PlanNodeId("write-log-test"),
            Collections.singletonList(0),
            Collections.singletonList(insertRowNode));

    final Logger logger = (Logger) LoggerFactory.getLogger(DataExecutionVisitor.class);
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      final TSStatus status =
          new DataExecutionVisitor().visitInsertRows(insertRowsNode, dataRegion);

      Assert.assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), status.getCode());
      Assert.assertEquals(failure.getMessage(), status.getMessage());
      Assert.assertEquals(1, appender.list.size());
      Assert.assertNotNull(appender.list.get(0).getThrowableProxy());
      Assert.assertEquals(
          failure.getClass().getName(), appender.list.get(0).getThrowableProxy().getClassName());
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  @Test
  public void testTreeDeleteFailureReturnsMessage() throws Exception {
    final DataRegion dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.sg");
    final DeleteDataNode node = createDeleteDataNode("root.sg.device.s1");
    final IOException failure = new IOException("injected tree delete failure");
    Mockito.doThrow(failure).when(dataRegion).deleteByDevice(any(MeasurementPath.class), any());

    final TSStatus status = new DataExecutionVisitor().visitDeleteData(node, dataRegion);

    Assert.assertEquals(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(failure.getMessage(), status.getMessage());
    Mockito.verify(dataRegion).writeUnlock();
  }

  @Test
  public void testDirectTreeDeleteFailureReturnsMessage() throws Exception {
    final DataRegion dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.sg");
    final DeleteDataNode node = createDeleteDataNode("root.sg.**");
    final IOException failure = new IOException("injected direct delete failure");
    Mockito.doThrow(failure).when(dataRegion).deleteDataDirectly(any(MeasurementPath.class), any());

    final TSStatus status = new DataExecutionVisitor().visitDeleteData(node, dataRegion);

    Assert.assertEquals(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(failure.getMessage(), status.getMessage());
    Mockito.verify(dataRegion).writeUnlock();
  }

  @Test
  public void testTableDeleteFailureReturnsMessage() throws Exception {
    final DataRegion dataRegion = Mockito.mock(DataRegion.class);
    final RelationalDeleteDataNode node =
        new RelationalDeleteDataNode(
            new PlanNodeId("delete-error-test"), Collections.emptyList(), "test_db");
    final IOException failure = new IOException("injected table delete failure");
    Mockito.doThrow(failure).when(dataRegion).deleteByTable(node);

    final TSStatus status = new DataExecutionVisitor().visitDeleteData(node, dataRegion);

    Assert.assertEquals(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(failure.getMessage(), status.getMessage());
  }

  private DeleteDataNode createDeleteDataNode(final String path) throws IllegalPathException {
    return new DeleteDataNode(
        new PlanNodeId("delete-error-test"),
        Collections.singletonList(new MeasurementPath(path)),
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }
}
