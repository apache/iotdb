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
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest(LoadFallbackHandler.class)
public class LoadFallbackHandlerTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LoadTsFileDataTypeConverter converter;

  @Before
  public void setUp() throws Exception {
    // Progress-log formatting is unrelated to conversion results. Avoid instrumenting JDK Stream
    // internals through this helper when running PowerMock on Java 17.
    PowerMockito.suppress(
        PowerMockito.method(LoadFallbackHandler.class, "getFailedFilePathsString"));
    converter = mock(LoadTsFileDataTypeConverter.class);
    when(converter.isSuccessful(any())).thenCallRealMethod();
    PowerMockito.whenNew(LoadTsFileDataTypeConverter.class)
        .withAnyArguments()
        .thenReturn(converter);
  }

  @Test
  public void testPartialFailureReportsEachFilesReason() throws Exception {
    final List<LoadSingleTsFileNode> nodes =
        Arrays.asList(
            newNode("success.tsfile", false),
            newNode("denied.tsfile", true),
            newNode("exception.tsfile", false),
            newNode("no-status.tsfile", false));
    final TSStatus denied =
        new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
            .setSubStatus(
                Collections.singletonList(
                    new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
                        .setMessage("missing WRITE_DATA privilege")));
    when(converter.convertForTableModel(any())).thenReturn(Optional.of(denied));
    when(converter.convertForTreeModel(any()))
        .thenReturn(Optional.of(RpcUtils.SUCCESS_STATUS))
        .thenThrow(new IllegalStateException())
        .thenReturn(Optional.empty());
    final List<Integer> failedIndexes = new ArrayList<>(Arrays.asList(0, 1, 2, 3));
    final QueryStateMachine stateMachine = newStateMachine();

    new LoadFallbackHandler(mock(MPPQueryContext.class), false, nodes, failedIndexes, stateMachine)
        .convertFailedTsFilesToTablets();

    Assert.assertEquals(QueryState.FAILED, stateMachine.getState());
    Assert.assertEquals(Arrays.asList(1, 2, 3), failedIndexes);
    final TSStatus failure = stateMachine.getFailureStatus();
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), failure.getCode());
    final String message = failure.getMessage();
    Assert.assertFalse(message.contains("success.tsfile"));
    for (int i = 1; i < nodes.size(); i++) {
      Assert.assertTrue(message.contains(nodes.get(i).getTsFileResource().getTsFilePath()));
    }
    Assert.assertTrue(message.contains("missing WRITE_DATA privilege"));
    Assert.assertTrue(message.contains(String.valueOf(TSStatusCode.NO_PERMISSION.getStatusCode())));
    Assert.assertTrue(message.contains(IllegalStateException.class.getName()));
    Assert.assertTrue(
        message.contains(
            DataNodeQueryMessages.MESSAGE_TABLET_CONVERSION_RETURNED_NO_STATUS_88BAB5D3));
  }

  @Test
  public void testSuccessfulConversionsStillFinishLoad() throws Exception {
    final List<LoadSingleTsFileNode> nodes =
        Arrays.asList(newNode("tree.tsfile", false), newNode("table.tsfile", true));
    when(converter.convertForTreeModel(any())).thenReturn(Optional.of(RpcUtils.SUCCESS_STATUS));
    when(converter.convertForTableModel(any())).thenReturn(Optional.of(RpcUtils.SUCCESS_STATUS));
    final List<Integer> failedIndexes = new ArrayList<>(Arrays.asList(0, 1));
    final QueryStateMachine stateMachine = newStateMachine();

    new LoadFallbackHandler(mock(MPPQueryContext.class), false, nodes, failedIndexes, stateMachine)
        .convertFailedTsFilesToTablets();

    Assert.assertEquals(QueryState.FINISHED, stateMachine.getState());
    Assert.assertTrue(failedIndexes.isEmpty());
  }

  private LoadSingleTsFileNode newNode(final String name, final boolean tableModel)
      throws Exception {
    final File file = temporaryFolder.newFile(name);
    final LoadSingleTsFileNode node = mock(LoadSingleTsFileNode.class);
    when(node.getTsFileResource()).thenReturn(new TsFileResource(file));
    when(node.isTableModel()).thenReturn(tableModel);
    when(node.getDatabase()).thenReturn(tableModel ? "test" : null);
    return node;
  }

  private QueryStateMachine newStateMachine() {
    final QueryStateMachine stateMachine =
        new QueryStateMachine(
            new QueryId("load_fallback"), MoreExecutors.newDirectExecutorService());
    stateMachine.transitionToRunning();
    return stateMachine;
  }
}
