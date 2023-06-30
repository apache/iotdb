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

package org.apache.iotdb.db.queryengine.execution.executor;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.storageengine.dataregion.VirtualDataRegion;

import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.iotdb.db.queryengine.common.QueryId.MOCK_QUERY_ID;
import static org.apache.iotdb.db.queryengine.execution.executor.RegionReadExecutor.ERROR_MSG_FORMAT;
import static org.apache.iotdb.db.queryengine.execution.executor.RegionReadExecutor.RESPONSE_NULL_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState.RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RegionReadExecutorTest {

  @Test
  public void testSuccessfulExecute() {

    // data query
    ConsensusGroupId dataRegionGroupId = new DataRegionId(1);
    FragmentInstanceId fragmentInstanceId =
        new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
    FragmentInstance fragmentInstance = Mockito.mock(FragmentInstance.class);
    Mockito.when(fragmentInstance.getId()).thenReturn(fragmentInstanceId);

    IConsensus dataRegionConsensus = Mockito.mock(IConsensus.class);
    IConsensus schemaRegionConsensus = Mockito.mock(IConsensus.class);
    FragmentInstanceManager fragmentInstanceManager = Mockito.mock(FragmentInstanceManager.class);

    RegionReadExecutor executor =
        new RegionReadExecutor(dataRegionConsensus, schemaRegionConsensus, fragmentInstanceManager);

    ConsensusReadResponse readResponse = Mockito.mock(ConsensusReadResponse.class);
    Mockito.when(readResponse.isSuccess()).thenReturn(true);
    FragmentInstanceInfo fragmentInstanceInfo = Mockito.mock(FragmentInstanceInfo.class);
    Mockito.when(readResponse.getDataset()).thenReturn(fragmentInstanceInfo);
    Mockito.when(fragmentInstanceInfo.getState()).thenReturn(RUNNING);
    Mockito.when(fragmentInstanceInfo.getMessage()).thenReturn("data-success");

    Mockito.when(dataRegionConsensus.read(dataRegionGroupId, fragmentInstance))
        .thenReturn(readResponse);

    RegionExecutionResult res = executor.execute(dataRegionGroupId, fragmentInstance);

    assertTrue(res.isAccepted());
    assertEquals("data-success", res.getMessage());

    // schema query
    ConsensusGroupId schemaRegionGroupId = new SchemaRegionId(1);

    Mockito.when(readResponse.isSuccess()).thenReturn(true);
    Mockito.when(readResponse.getDataset()).thenReturn(fragmentInstanceInfo);
    Mockito.when(fragmentInstanceInfo.getState()).thenReturn(RUNNING);
    Mockito.when(fragmentInstanceInfo.getMessage()).thenReturn("schema-success");

    Mockito.when(schemaRegionConsensus.read(schemaRegionGroupId, fragmentInstance))
        .thenReturn(readResponse);

    res = executor.execute(schemaRegionGroupId, fragmentInstance);

    assertTrue(res.isAccepted());
    assertEquals("schema-success", res.getMessage());
  }

  @Test
  public void testResponseNullExecute() {

    // data query
    ConsensusGroupId dataRegionGroupId = new DataRegionId(1);
    FragmentInstanceId fragmentInstanceId =
        new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
    FragmentInstance fragmentInstance = Mockito.mock(FragmentInstance.class);
    Mockito.when(fragmentInstance.getId()).thenReturn(fragmentInstanceId);

    IConsensus dataRegionConsensus = Mockito.mock(IConsensus.class);
    IConsensus schemaRegionConsensus = Mockito.mock(IConsensus.class);
    FragmentInstanceManager fragmentInstanceManager = Mockito.mock(FragmentInstanceManager.class);

    RegionReadExecutor executor =
        new RegionReadExecutor(dataRegionConsensus, schemaRegionConsensus, fragmentInstanceManager);

    Mockito.when(dataRegionConsensus.read(dataRegionGroupId, fragmentInstance)).thenReturn(null);

    RegionExecutionResult res = executor.execute(dataRegionGroupId, fragmentInstance);

    assertFalse(res.isAccepted());
    assertEquals(RESPONSE_NULL_ERROR_MSG, res.getMessage());

    // schema query
    ConsensusGroupId schemaRegionGroupId = new SchemaRegionId(1);

    Mockito.when(schemaRegionConsensus.read(schemaRegionGroupId, fragmentInstance))
        .thenReturn(null);

    res = executor.execute(schemaRegionGroupId, fragmentInstance);

    assertFalse(res.isAccepted());
    assertEquals(RESPONSE_NULL_ERROR_MSG, res.getMessage());
  }

  @Test
  public void testFailedExecute() {

    // data query
    ConsensusGroupId dataRegionGroupId = new DataRegionId(1);
    FragmentInstanceId fragmentInstanceId =
        new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
    FragmentInstance fragmentInstance = Mockito.mock(FragmentInstance.class);
    Mockito.when(fragmentInstance.getId()).thenReturn(fragmentInstanceId);

    IConsensus dataRegionConsensus = Mockito.mock(IConsensus.class);
    IConsensus schemaRegionConsensus = Mockito.mock(IConsensus.class);
    FragmentInstanceManager fragmentInstanceManager = Mockito.mock(FragmentInstanceManager.class);

    RegionReadExecutor executor =
        new RegionReadExecutor(dataRegionConsensus, schemaRegionConsensus, fragmentInstanceManager);

    ConsensusReadResponse readResponse = Mockito.mock(ConsensusReadResponse.class);
    Mockito.when(readResponse.isSuccess()).thenReturn(false);
    Mockito.when(readResponse.getException()).thenReturn(new ConsensusException("data-exception"));

    Mockito.when(dataRegionConsensus.read(dataRegionGroupId, fragmentInstance))
        .thenReturn(readResponse);

    RegionExecutionResult res = executor.execute(dataRegionGroupId, fragmentInstance);

    assertFalse(res.isAccepted());
    assertEquals(String.format(ERROR_MSG_FORMAT, "data-exception"), res.getMessage());

    // schema query
    ConsensusGroupId schemaRegionGroupId = new SchemaRegionId(1);

    Mockito.when(readResponse.isSuccess()).thenReturn(false);
    Mockito.when(readResponse.getException()).thenReturn(null);

    Mockito.when(schemaRegionConsensus.read(schemaRegionGroupId, fragmentInstance))
        .thenReturn(readResponse);

    res = executor.execute(schemaRegionGroupId, fragmentInstance);

    assertFalse(res.isAccepted());
    assertEquals(String.format(ERROR_MSG_FORMAT, ""), res.getMessage());
  }

  @Test
  public void testExceptionHappened() {

    ConsensusGroupId dataRegionGroupId = new DataRegionId(1);
    FragmentInstanceId fragmentInstanceId =
        new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
    FragmentInstance fragmentInstance = Mockito.mock(FragmentInstance.class);
    Mockito.when(fragmentInstance.getId()).thenReturn(fragmentInstanceId);

    IConsensus dataRegionConsensus = Mockito.mock(IConsensus.class);
    IConsensus schemaRegionConsensus = Mockito.mock(IConsensus.class);
    FragmentInstanceManager fragmentInstanceManager = Mockito.mock(FragmentInstanceManager.class);

    RegionReadExecutor executor =
        new RegionReadExecutor(dataRegionConsensus, schemaRegionConsensus, fragmentInstanceManager);

    Mockito.when(dataRegionConsensus.read(dataRegionGroupId, fragmentInstance))
        .thenThrow(new RuntimeException("Unknown"));

    RegionExecutionResult res = executor.execute(dataRegionGroupId, fragmentInstance);

    assertFalse(res.isAccepted());
    assertEquals(String.format(ERROR_MSG_FORMAT, "Unknown"), res.getMessage());
  }

  @Test
  public void testVirtualDataRegion() {
    // successfully
    FragmentInstanceId fragmentInstanceId =
        new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
    FragmentInstance fragmentInstance = Mockito.mock(FragmentInstance.class);
    Mockito.when(fragmentInstance.getId()).thenReturn(fragmentInstanceId);

    IConsensus dataRegionConsensus = Mockito.mock(IConsensus.class);
    IConsensus schemaRegionConsensus = Mockito.mock(IConsensus.class);
    FragmentInstanceManager fragmentInstanceManager = Mockito.mock(FragmentInstanceManager.class);

    RegionReadExecutor executor =
        new RegionReadExecutor(dataRegionConsensus, schemaRegionConsensus, fragmentInstanceManager);

    FragmentInstanceInfo fragmentInstanceInfo = Mockito.mock(FragmentInstanceInfo.class);
    Mockito.when(fragmentInstanceInfo.getState()).thenReturn(RUNNING);
    Mockito.when(fragmentInstanceInfo.getMessage()).thenReturn("data-success");

    Mockito.when(
            fragmentInstanceManager.execDataQueryFragmentInstance(
                fragmentInstance, VirtualDataRegion.getInstance()))
        .thenReturn(fragmentInstanceInfo);

    RegionExecutionResult res = executor.execute(fragmentInstance);

    assertTrue(res.isAccepted());
    assertEquals("data-success", res.getMessage());

    // failure

    Mockito.when(
            fragmentInstanceManager.execDataQueryFragmentInstance(
                fragmentInstance, VirtualDataRegion.getInstance()))
        .thenThrow(new RuntimeException("Unknown"));

    res = executor.execute(fragmentInstance);

    assertFalse(res.isAccepted());
    assertEquals(String.format(ERROR_MSG_FORMAT, "Unknown"), res.getMessage());
  }
}
