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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.TTLManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.state.schema.SetTTLState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SetTTLProcedureTest {

  @Test
  public void serializeDeserializeTest() throws IOException, IllegalPathException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    // test1
    PartialPath path = new PartialPath("root.test.sg1.group1.group1.**");
    SetTTLPlan setTTLPlan = new SetTTLPlan(Arrays.asList(path.getNodes()), 1928300234200L);
    SetTTLProcedure proc = new SetTTLProcedure(setTTLPlan, false);

    proc.serialize(outputStream);
    ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    SetTTLProcedure proc2 = (SetTTLProcedure) ProcedureFactory.getInstance().create(buffer);
    Assert.assertTrue(proc.equals(proc2));
    buffer.clear();
    byteArrayOutputStream.reset();

    // test2
    path = new PartialPath("root.**");
    setTTLPlan = new SetTTLPlan(Arrays.asList(path.getNodes()), -1);
    proc = new SetTTLProcedure(setTTLPlan, false);

    proc.serialize(outputStream);
    buffer = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    proc2 = (SetTTLProcedure) ProcedureFactory.getInstance().create(buffer);
    Assert.assertTrue(proc.equals(proc2));
    buffer.clear();
    byteArrayOutputStream.reset();
  }

  @Test
  public void serializeDeserializeTestWithCapturedRollbackState() throws Exception {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    final SetTTLPlan setTTLPlan =
        new SetTTLPlan(Arrays.asList(new PartialPath("root.db").getNodes()), 2000L);
    setTTLPlan.setDataBase(true);
    final TestingSetTTLProcedure procedure = new TestingSetTTLProcedure(setTTLPlan);

    final Map<String, Long> ttlMap = new HashMap<>();
    ttlMap.put("root.**", Long.MAX_VALUE);
    ttlMap.put("root.db", 500L);
    ttlMap.put("root.db.**", 600L);

    procedure.executeFromState(mockProcedureEnv(ttlMap), SetTTLState.CAPTURE_PREVIOUS_TTL);

    procedure.serialize(outputStream);
    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    final SetTTLProcedure deserializedProcedure =
        (SetTTLProcedure) ProcedureFactory.getInstance().create(buffer);
    assertSerializedProcedure(
        deserializedProcedure, "root.db", 2000L, true, true, 500L, 600L, false);
  }

  @Test
  public void deserializeOldFormatWithoutRollbackStateTest() throws Exception {
    final SetTTLPlan setTTLPlan =
        new SetTTLPlan(Arrays.asList(new PartialPath("root.db").getNodes()), 2000L);
    setTTLPlan.setDataBase(true);

    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    writeOldFormatProcedure(outputStream, setTTLPlan);

    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    final SetTTLProcedure deserializedProcedure =
        (SetTTLProcedure) ProcedureFactory.getInstance().create(buffer);

    assertSerializedProcedure(
        deserializedProcedure,
        "root.db",
        2000L,
        true,
        false,
        Long.MIN_VALUE,
        Long.MIN_VALUE,
        false);
  }

  @Test
  public void setConfigNodeTTLShouldNotWriteBeforePreviousStateIsCaptured() throws Exception {
    final SetTTLPlan setTTLPlan =
        new SetTTLPlan(Arrays.asList(new PartialPath("root.db").getNodes()), 2000L);
    setTTLPlan.setDataBase(true);
    final TestingSetTTLProcedure procedure = new TestingSetTTLProcedure(setTTLPlan);

    final Map<String, Long> ttlMap = new HashMap<>();
    ttlMap.put("root.**", Long.MAX_VALUE);
    ttlMap.put("root.db", 500L);
    ttlMap.put("root.db.**", 600L);

    procedure.executeFromState(mockProcedureEnv(ttlMap), SetTTLState.SET_CONFIGNODE_TTL);

    Assert.assertTrue(procedure.getWrittenPlans().isEmpty());
    assertSerializedProcedure(procedure, "root.db", 2000L, true, true, 500L, 600L, false);

    procedure.executeFromState(mockProcedureEnv(ttlMap), SetTTLState.SET_CONFIGNODE_TTL);

    Assert.assertEquals(1, procedure.getWrittenPlans().size());
    assertPlan(procedure.getWrittenPlans().get(0), "root.db", 2000L, true);
  }

  @Test
  public void rollbackStateShouldUnsetNewTTLWhenPreviousStateDidNotExist() throws Exception {
    final SetTTLPlan setTTLPlan =
        new SetTTLPlan(Arrays.asList(new PartialPath("root.test.sg1.**").getNodes()), 1000L);
    final TestingSetTTLProcedure procedure = new TestingSetTTLProcedure(setTTLPlan);
    procedure.failFirstDataNodeUpdateForTest();

    final ConfigNodeProcedureEnv env =
        mockProcedureEnv(Collections.singletonMap("root.**", Long.MAX_VALUE));

    procedure.executeFromState(env, SetTTLState.CAPTURE_PREVIOUS_TTL);
    procedure.executeFromState(env, SetTTLState.SET_CONFIGNODE_TTL);
    procedure.executeFromState(env, SetTTLState.UPDATE_DATANODE_CACHE);
    Assert.assertTrue(procedure.isFailed());

    procedure.rollbackState(env, SetTTLState.UPDATE_DATANODE_CACHE);

    Assert.assertEquals(2, procedure.getWrittenPlans().size());
    assertPlan(procedure.getWrittenPlans().get(0), "root.test.sg1.**", 1000L, false);
    assertPlan(procedure.getWrittenPlans().get(1), "root.test.sg1.**", -1L, false);

    Assert.assertEquals(2, procedure.getRequests().size());
    assertRequest(procedure.getRequests().get(0), "root.test.sg1.**", 1000L, false);
    assertRequest(procedure.getRequests().get(1), "root.test.sg1.**", -1L, false);
  }

  @Test
  public void rollbackStateShouldRestoreDatabaseWildcardTTLSeparately() throws Exception {
    final SetTTLPlan setTTLPlan =
        new SetTTLPlan(Arrays.asList(new PartialPath("root.db").getNodes()), 2000L);
    setTTLPlan.setDataBase(true);
    final TestingSetTTLProcedure procedure = new TestingSetTTLProcedure(setTTLPlan);
    procedure.failFirstDataNodeUpdateForTest();

    final Map<String, Long> ttlMap = new HashMap<>();
    ttlMap.put("root.**", Long.MAX_VALUE);
    ttlMap.put("root.db", 500L);
    ttlMap.put("root.db.**", 600L);
    final ConfigNodeProcedureEnv env = mockProcedureEnv(ttlMap);

    procedure.executeFromState(env, SetTTLState.CAPTURE_PREVIOUS_TTL);
    procedure.executeFromState(env, SetTTLState.SET_CONFIGNODE_TTL);
    procedure.executeFromState(env, SetTTLState.UPDATE_DATANODE_CACHE);
    Assert.assertTrue(procedure.isFailed());

    procedure.rollbackState(env, SetTTLState.UPDATE_DATANODE_CACHE);

    Assert.assertEquals(3, procedure.getWrittenPlans().size());
    assertPlan(procedure.getWrittenPlans().get(0), "root.db", 2000L, true);
    assertPlan(procedure.getWrittenPlans().get(1), "root.db", 500L, false);
    assertPlan(procedure.getWrittenPlans().get(2), "root.db.**", 600L, false);

    Assert.assertEquals(3, procedure.getRequests().size());
    assertRequest(procedure.getRequests().get(0), "root.db", 2000L, true);
    assertRequest(procedure.getRequests().get(1), "root.db", 500L, false);
    assertRequest(procedure.getRequests().get(2), "root.db.**", 600L, false);
  }

  private ConfigNodeProcedureEnv mockProcedureEnv(final Map<String, Long> ttlMap) {
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final TTLManager ttlManager = Mockito.mock(TTLManager.class);
    final NodeManager nodeManager = Mockito.mock(NodeManager.class);

    final TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(1);

    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getTTLManager()).thenReturn(ttlManager);
    Mockito.when(ttlManager.getTTL(Mockito.any(String[].class)))
        .thenAnswer(
            invocation -> {
              final String[] pathPattern = invocation.getArgument(0);
              return ttlMap.getOrDefault(String.join(".", pathPattern), TTLCache.NULL_TTL);
            });
    Mockito.when(configManager.getNodeManager()).thenReturn(nodeManager);
    Mockito.when(nodeManager.getRegisteredDataNodeLocations())
        .thenReturn(Collections.singletonMap(1, dataNodeLocation));
    return env;
  }

  private void assertPlan(
      final SetTTLPlan plan, final String path, final long ttl, final boolean isDataBase) {
    Assert.assertEquals(path, String.join(".", plan.getPathPattern()));
    Assert.assertEquals(ttl, plan.getTTL());
    Assert.assertEquals(isDataBase, plan.isDataBase());
  }

  private void assertRequest(
      final TSetTTLReq req, final String path, final long ttl, final boolean isDataBase) {
    Assert.assertEquals(Collections.singletonList(path), req.getPathPattern());
    Assert.assertEquals(ttl, req.getTTL());
    Assert.assertEquals(isDataBase, req.isDataBase);
  }

  private void writeOldFormatProcedure(final DataOutputStream stream, final SetTTLPlan plan)
      throws IOException {
    stream.writeShort(ProcedureType.SET_TTL_PROCEDURE.getTypeCode());
    // Procedure fields.
    stream.writeLong(Procedure.NO_PROC_ID);
    stream.writeInt(ProcedureState.INITIALIZING.ordinal());
    stream.writeLong(0L);
    stream.writeLong(0L);
    stream.writeLong(Procedure.NO_PROC_ID);
    stream.writeLong(Procedure.NO_TIMEOUT);
    stream.writeInt(-1); // no stack indexes
    stream.write((byte) 0); // no exception
    stream.writeInt(-1); // no result
    stream.write((byte) 0); // no lock
    // StateMachineProcedure fields.
    stream.writeInt(0); // no states
    ReadWriteIOUtils.write(plan.serializeToByteBuffer(), stream);
  }

  private void assertSerializedProcedure(
      final SetTTLProcedure procedure,
      final String path,
      final long ttl,
      final boolean isDataBase,
      final boolean previousTTLStateCaptured,
      final long previousTTL,
      final long previousDatabaseWildcardTTL,
      final boolean isGeneratedByPipe)
      throws Exception {
    final Field planField = findField(SetTTLProcedure.class, "plan");
    planField.setAccessible(true);
    assertPlan((SetTTLPlan) planField.get(procedure), path, ttl, isDataBase);

    final Field previousTTLStateCapturedField =
        findField(SetTTLProcedure.class, "previousTTLStateCaptured");
    previousTTLStateCapturedField.setAccessible(true);
    Assert.assertEquals(previousTTLStateCaptured, previousTTLStateCapturedField.get(procedure));

    final Field previousTTLField = findField(SetTTLProcedure.class, "previousTTL");
    previousTTLField.setAccessible(true);
    Assert.assertEquals(previousTTL, previousTTLField.get(procedure));

    final Field previousDatabaseWildcardTTLField =
        findField(SetTTLProcedure.class, "previousDatabaseWildcardTTL");
    previousDatabaseWildcardTTLField.setAccessible(true);
    Assert.assertEquals(
        previousDatabaseWildcardTTL, previousDatabaseWildcardTTLField.get(procedure));

    final Field isGeneratedByPipeField = findField(SetTTLProcedure.class, "isGeneratedByPipe");
    isGeneratedByPipeField.setAccessible(true);
    Assert.assertEquals(isGeneratedByPipe, isGeneratedByPipeField.get(procedure));
  }

  private Field findField(final Class<?> clazz, final String fieldName)
      throws NoSuchFieldException {
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  private static class TestingSetTTLProcedure extends SetTTLProcedure {

    private final List<TSetTTLReq> requests = new ArrayList<>();
    private final List<SetTTLPlan> writtenPlans = new ArrayList<>();
    private boolean failFirstDataNodeUpdate = false;
    private int requestCount = 0;

    private TestingSetTTLProcedure(final SetTTLPlan plan) {
      super(plan, false);
    }

    private void failFirstDataNodeUpdateForTest() {
      failFirstDataNodeUpdate = true;
    }

    private List<TSetTTLReq> getRequests() {
      return requests;
    }

    private List<SetTTLPlan> getWrittenPlans() {
      return writtenPlans;
    }

    @Override
    TSStatus writeConfigNodePlan(final ConfigNodeProcedureEnv env, final SetTTLPlan setTTLPlan) {
      writtenPlans.add(copyPlan(setTTLPlan));
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    @Override
    boolean broadcastTTLAndDecide(final ConfigNodeProcedureEnv env, final TSetTTLReq req) {
      requests.add(copyRequest(req));
      // Simulate a live, un-acked DataNode on the first broadcast: the propagator verdict is FAIL
      // (which triggers rollback). Later broadcasts (the rollback restore) proceed.
      return !(failFirstDataNodeUpdate && requestCount++ == 0);
    }

    private SetTTLPlan copyPlan(final SetTTLPlan plan) {
      final SetTTLPlan copiedPlan =
          new SetTTLPlan(Arrays.asList(plan.getPathPattern()), plan.getTTL());
      copiedPlan.setDataBase(plan.isDataBase());
      return copiedPlan;
    }

    private TSetTTLReq copyRequest(final TSetTTLReq req) {
      return new TSetTTLReq(new ArrayList<>(req.getPathPattern()), req.getTTL(), req.isDataBase);
    }
  }
}
