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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TabletRewriteLoadStrategyTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LoadTsFileDataTypeConverter converter;
  private LoadTsFileCostMetricsSet costMetrics;
  private LoadSingleTsFileNode node;
  private File sourceFile;

  @Before
  public void setUp() throws Exception {
    converter = mock(LoadTsFileDataTypeConverter.class);
    when(converter.isSuccessful(any())).thenCallRealMethod();
    costMetrics = mock(LoadTsFileCostMetricsSet.class);
    sourceFile = temporaryFolder.newFile("source.tsfile");
    node = mock(LoadSingleTsFileNode.class);
    when(node.getTsFileResource()).thenReturn(new TsFileResource(sourceFile));
    when(node.isDeleteAfterLoad()).thenReturn(true);
  }

  @Test
  public void testTreePipeRewritePreservesDatabaseAndLeavesDeletionToScheduler() throws Exception {
    when(node.getDatabase()).thenReturn("root.test.sg");
    when(converter.convertForTreeModel(any()))
        .thenReturn(Optional.of(status(TSStatusCode.SUCCESS_STATUS)));

    assertTrue(strategy(true).execute(node));

    final ArgumentCaptor<LoadTsFileStatement> statement =
        ArgumentCaptor.forClass(LoadTsFileStatement.class);
    verify(converter).convertForTreeModel(statement.capture());
    assertEquals("root.test.sg", statement.getValue().getDatabase());
    assertEquals(2, statement.getValue().getDatabaseLevel());
    assertTrue(statement.getValue().isGeneratedByPipe());
    assertFalse(statement.getValue().isDeleteAfterLoad());
    assertEquals(Collections.singletonList(sourceFile), statement.getValue().getTsFiles());
    assertTrue(sourceFile.exists());
    verify(node, never()).clean();
    verifyMetric();
  }

  @Test
  public void testFailedConversionRetainsSourceAndRecordsMetric() throws Exception {
    when(converter.convertForTreeModel(any()))
        .thenReturn(Optional.of(status(TSStatusCode.LOAD_FILE_ERROR)));

    assertFalse(strategy(false).execute(node));

    final ArgumentCaptor<LoadTsFileStatement> statement =
        ArgumentCaptor.forClass(LoadTsFileStatement.class);
    verify(converter).convertForTreeModel(statement.capture());
    assertFalse(statement.getValue().isDeleteAfterLoad());
    assertFalse(statement.getValue().isGeneratedByPipe());
    verify(node, never()).clean();
    assertTrue(sourceFile.exists());
    verifyMetric();
  }

  @Test
  public void testMissingConversionStatusFailsWithoutDeletingSource() throws Exception {
    when(converter.convertForTreeModel(any())).thenReturn(Optional.empty());

    assertFalse(strategy(false).execute(node));
    assertTrue(sourceFile.exists());
    verify(node, never()).clean();
    verifyMetric();
  }

  @Test
  public void testConversionExceptionIsWrappedAndRetainsSource() {
    final IllegalStateException cause = new IllegalStateException("conversion failed");
    when(converter.convertForTreeModel(any())).thenThrow(cause);

    final LoadFileException failure =
        assertThrows(LoadFileException.class, () -> strategy(false).execute(node));

    assertSame(cause, failure.getCause());
    assertTrue(sourceFile.exists());
    verify(node, never()).clean();
    verifyMetric();
  }

  @Test
  public void testFallbackConversionStillHonorsItsDeleteAfterLoadArgument() throws Exception {
    when(converter.convertForTreeModel(any()))
        .thenReturn(Optional.of(status(TSStatusCode.SUCCESS_STATUS)));

    LoadFallbackHandler.convertTsFileToTablets(converter, node, false, true);

    final ArgumentCaptor<LoadTsFileStatement> statement =
        ArgumentCaptor.forClass(LoadTsFileStatement.class);
    verify(converter).convertForTreeModel(statement.capture());
    assertTrue(statement.getValue().isDeleteAfterLoad());
  }

  private TabletRewriteLoadStrategy strategy(final boolean isGeneratedByPipe) {
    return new TabletRewriteLoadStrategy(converter, isGeneratedByPipe, costMetrics);
  }

  @Test
  public void testTablePipeRewritePreservesDatabaseAndDefersDeletion() throws Exception {
    when(node.isTableModel()).thenReturn(true);
    when(node.getDatabase()).thenReturn("table_db");
    when(converter.convertForTableModel(any()))
        .thenReturn(Optional.of(status(TSStatusCode.SUCCESS_STATUS)));
    assertTrue(strategy(true).execute(node));
    ArgumentCaptor<LoadTsFile> statement = ArgumentCaptor.forClass(LoadTsFile.class);
    verify(converter).convertForTableModel(statement.capture());
    assertEquals("table_db", statement.getValue().getDatabase());
    assertTrue(statement.getValue().isGeneratedByPipe());
    assertFalse(statement.getValue().isDeleteAfterLoad());
    assertTrue(sourceFile.exists());
  }

  private void verifyMetric() {
    verify(costMetrics)
        .recordPhaseTimeCost(eq(LoadTsFileCostMetricsSet.SCHEDULER_CAST_TABLETS), anyLong());
  }

  private static TSStatus status(final TSStatusCode code) {
    return new TSStatus(code.getStatusCode());
  }
}
