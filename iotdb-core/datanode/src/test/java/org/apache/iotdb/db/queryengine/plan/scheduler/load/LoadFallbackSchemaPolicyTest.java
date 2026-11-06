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
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest(LoadTsFileScheduler.class)
public class LoadFallbackSchemaPolicyTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testTreeFallbackPreservesRequestSchemaCreationSetting() throws Exception {
    final LoadTsFileDataTypeConverter converter = mock(LoadTsFileDataTypeConverter.class);
    when(converter.convertForTreeModel(any()))
        .thenReturn(Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    when(converter.isSuccessful(any())).thenCallRealMethod();
    PowerMockito.whenNew(LoadTsFileDataTypeConverter.class)
        .withAnyArguments()
        .thenReturn(converter);
    final File file = temporaryFolder.newFile("tree-schema.tsfile");
    for (final boolean generatedByPipe : new boolean[] {false, true}) {
      for (final boolean requested : new boolean[] {false, true, false}) {
        clearInvocations(converter);
        final LoadTsFileStatement original = LoadTsFileStatement.createUnchecked(file.getPath());
        original.setAutoCreateSchema(requested);
        original.setDeleteAfterLoad(true);
        original.addWritePointCount(1);
        final Analysis analysis = new Analysis();
        analysis.setStatement(generatedByPipe ? new PipeEnrichedStatement(original) : original);
        final LoadSingleTsFileNode node =
            (LoadSingleTsFileNode)
                new LoadTsFileNode(
                        new PlanNodeId("load_schema"),
                        Collections.singletonList(new TsFileResource(file)))
                    .splitByPartition(analysis)
                    .get(0);
        final DistributedQueryPlan plan = mock(DistributedQueryPlan.class, RETURNS_DEEP_STUBS);
        when(plan.getRootSubPlan().getPlanFragment().getId())
            .thenReturn(new PlanFragmentId("test", 0));
        when(plan.getInstances()).thenReturn(Collections.emptyList());
        final QueryStateMachine state = mock(QueryStateMachine.class);
        final LoadTsFileScheduler scheduler =
            new LoadTsFileScheduler(
                plan,
                mock(MPPQueryContext.class),
                state,
                mock(IClientManager.class),
                mock(IPartitionFetcher.class),
                generatedByPipe,
                "root.test");
        try {
          final List<LoadSingleTsFileNode> nodes =
              Whitebox.getInternalState(scheduler, "tsFileNodeList");
          final List<Integer> failed =
              Whitebox.getInternalState(scheduler, "failedTsFileNodeIndexes");
          nodes.add(node);
          failed.add(0);
          Whitebox.invokeMethod(scheduler, "convertFailedTsFilesToTabletsAndRetry");
          verify(state).transitionToFinished();
          Assert.assertTrue(failed.isEmpty());
          final ArgumentCaptor<LoadTsFileStatement> statement =
              ArgumentCaptor.forClass(LoadTsFileStatement.class);
          verify(converter).convertForTreeModel(statement.capture());
          Assert.assertEquals(requested, statement.getValue().isAutoCreateSchema());
          Assert.assertEquals(generatedByPipe, statement.getValue().isGeneratedByPipe());
          Assert.assertTrue(statement.getValue().isDeleteAfterLoad());
          Assert.assertEquals(Collections.singletonList(file), statement.getValue().getTsFiles());
          Assert.assertEquals("root.test", statement.getValue().getDatabase());
          Assert.assertTrue(file.exists());
        } finally {
          LoadTsFileMemoryManager.getInstance().releaseDataCacheMemoryBlock();
        }
      }
    }
  }
}
