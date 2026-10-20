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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DeleteDevicesProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProcedureManagerTableTaskTest {

  @Test
  public void testWaitingForDuplicateTableTaskDoesNotBlockOtherTableTask() throws Exception {
    final ProcedureExecutor<ConfigNodeProcedureEnv> procedureExecutor =
        mock(ProcedureExecutor.class);
    final ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> procedures =
        new ConcurrentHashMap<>();
    when(procedureExecutor.getProcedures()).thenReturn(procedures);

    final TestProcedureManager procedureManager =
        new TestProcedureManager(mock(ConfigManager.class), mock(ProcedureInfo.class));
    procedureManager.setExecutor(procedureExecutor);

    final String database = "database";
    final TsTable firstTable = new TsTable("first");
    final CreateTableProcedure runningProcedure =
        new CreateTableProcedure(database, firstTable, false);
    runningProcedure.setProcId(1);
    procedures.put(runningProcedure.getProcId(), runningProcedure);
    procedureManager.blockWhenWaitingFor(runningProcedure);

    final CreateTableProcedure duplicateProcedure =
        new CreateTableProcedure(database, firstTable, false);
    final TsTable secondTable = new TsTable("second");
    final CreateTableProcedure independentProcedure =
        new CreateTableProcedure(database, secondTable, false);
    final ExecutorService requestExecutor = Executors.newFixedThreadPool(2);

    try {
      final Future<TSStatus> duplicateRequest =
          requestExecutor.submit(
              () ->
                  procedureManager.executeWithoutDuplicate(
                      database,
                      firstTable,
                      firstTable.getTableName(),
                      null,
                      ProcedureType.CREATE_TABLE_PROCEDURE,
                      duplicateProcedure));
      assertTrue(procedureManager.awaitBlockedWait());

      final Future<TSStatus> independentRequest =
          requestExecutor.submit(
              () ->
                  procedureManager.executeWithoutDuplicate(
                      database,
                      secondTable,
                      secondTable.getTableName(),
                      null,
                      ProcedureType.CREATE_TABLE_PROCEDURE,
                      independentProcedure));

      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          independentRequest.get(5, TimeUnit.SECONDS).getCode());
      verify(procedureExecutor).submitProcedure(independentProcedure);
      verify(procedureExecutor, never()).submitProcedure(duplicateProcedure);

      procedureManager.releaseBlockedWait();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          duplicateRequest.get(5, TimeUnit.SECONDS).getCode());
    } finally {
      procedureManager.releaseBlockedWait();
      requestExecutor.shutdownNow();
    }
  }

  @Test
  public void testWaitingForDuplicateDeleteDevicesDoesNotBlockOtherTableTask() throws Exception {
    final ProcedureExecutor<ConfigNodeProcedureEnv> procedureExecutor =
        mock(ProcedureExecutor.class);
    final ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> procedures =
        new ConcurrentHashMap<>();
    when(procedureExecutor.getProcedures()).thenReturn(procedures);

    final TestProcedureManager procedureManager =
        new TestProcedureManager(mock(ConfigManager.class), mock(ProcedureInfo.class));
    procedureManager.setExecutor(procedureExecutor);

    final String database = "database";
    final String tableName = "first";
    final String queryId = "query";
    final byte[] emptyBytes = new byte[0];
    final DeleteDevicesProcedure runningProcedure =
        new DeleteDevicesProcedure(
            database, tableName, queryId, emptyBytes, emptyBytes, emptyBytes, false);
    runningProcedure.setProcId(1);
    procedures.put(runningProcedure.getProcId(), runningProcedure);
    procedureManager.blockWhenWaitingFor(runningProcedure);

    final TDeleteTableDeviceReq duplicateRequest =
        new TDeleteTableDeviceReq(
            database,
            tableName,
            queryId,
            ByteBuffer.wrap(emptyBytes),
            ByteBuffer.wrap(emptyBytes),
            ByteBuffer.wrap(emptyBytes));
    final TsTable secondTable = new TsTable("second");
    final CreateTableProcedure independentProcedure =
        new CreateTableProcedure(database, secondTable, false);
    final ExecutorService requestExecutor = Executors.newFixedThreadPool(2);

    try {
      final Future<TDeleteTableDeviceResp> deleteDevicesRequest =
          requestExecutor.submit(() -> procedureManager.deleteDevices(duplicateRequest, false));
      assertTrue(procedureManager.awaitBlockedWait());

      final Future<TSStatus> independentRequest =
          requestExecutor.submit(
              () ->
                  procedureManager.executeWithoutDuplicate(
                      database,
                      secondTable,
                      secondTable.getTableName(),
                      null,
                      ProcedureType.CREATE_TABLE_PROCEDURE,
                      independentProcedure));

      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          independentRequest.get(5, TimeUnit.SECONDS).getCode());
      verify(procedureExecutor).submitProcedure(independentProcedure);

      procedureManager.releaseBlockedWait();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          deleteDevicesRequest.get(5, TimeUnit.SECONDS).getStatus().getCode());
    } finally {
      procedureManager.releaseBlockedWait();
      requestExecutor.shutdownNow();
    }
  }

  private static class TestProcedureManager extends ProcedureManager {

    private final CountDownLatch waitStarted = new CountDownLatch(1);
    private final CountDownLatch waitRelease = new CountDownLatch(1);
    private Procedure<?> blockedProcedure;

    private TestProcedureManager(
        final ConfigManager configManager, final ProcedureInfo procedureInfo) {
      super(configManager, procedureInfo);
    }

    private void blockWhenWaitingFor(final Procedure<?> procedure) {
      blockedProcedure = procedure;
    }

    private boolean awaitBlockedWait() throws InterruptedException {
      return waitStarted.await(5, TimeUnit.SECONDS);
    }

    private void releaseBlockedWait() {
      waitRelease.countDown();
    }

    @Override
    protected TSStatus waitingProcedureFinished(final Procedure<?> procedure) {
      if (procedure == blockedProcedure) {
        waitStarted.countDown();
        try {
          waitRelease.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return StatusUtils.OK;
    }
  }
}
