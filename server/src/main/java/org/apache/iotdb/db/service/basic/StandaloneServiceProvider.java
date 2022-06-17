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
package org.apache.iotdb.db.service.basic;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.doublelive.OperationSyncConsumer;
import org.apache.iotdb.db.doublelive.OperationSyncDDLProtector;
import org.apache.iotdb.db.doublelive.OperationSyncDMLProtector;
import org.apache.iotdb.db.doublelive.OperationSyncLogService;
import org.apache.iotdb.db.doublelive.OperationSyncPlanTypeUtils;
import org.apache.iotdb.db.doublelive.OperationSyncProducer;
import org.apache.iotdb.db.doublelive.OperationSyncWriteTask;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageEngineReadonlyException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StandaloneServiceProvider extends ServiceProvider {

  /* OperationSync module */
  private static final boolean isEnableOperationSync =
      IoTDBDescriptor.getInstance().getConfig().isEnableOperationSync();
  private final SessionPool operationSyncsessionPool;
  private final OperationSyncProducer operationSyncProducer;
  private final OperationSyncDDLProtector operationSyncDDLProtector;
  private final OperationSyncLogService operationSyncDDLLogService;

  public StandaloneServiceProvider() throws QueryProcessException {
    super(new PlanExecutor());
    if (isEnableOperationSync) {
      /* Open OperationSync */
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      // create SessionPool for OperationSync
      operationSyncsessionPool =
          new SessionPool(
              config.getSecondaryAddress(),
              config.getSecondaryPort(),
              config.getSecondaryUser(),
              config.getSecondaryPassword(),
              5);

      // create operationSyncDDLProtector and operationSyncDDLLogService
      operationSyncDDLProtector = new OperationSyncDDLProtector(operationSyncsessionPool);
      new Thread(operationSyncDDLProtector).start();
      operationSyncDDLLogService =
          new OperationSyncLogService("OperationSyncDDLLog", operationSyncDDLProtector);
      new Thread(operationSyncDDLLogService).start();

      // create OperationSyncProducer
      BlockingQueue<Pair<ByteBuffer, OperationSyncPlanTypeUtils.OperationSyncPlanType>>
          blockingQueue = new ArrayBlockingQueue<>(config.getOperationSyncProducerCacheSize());
      operationSyncProducer = new OperationSyncProducer(blockingQueue);

      // create OperationSyncDMLProtector and OperationSyncDMLLogService
      OperationSyncDMLProtector operationSyncDMLProtector =
          new OperationSyncDMLProtector(operationSyncDDLProtector, operationSyncProducer);
      new Thread(operationSyncDMLProtector).start();
      OperationSyncLogService operationSyncDMLLogService =
          new OperationSyncLogService("OperationSyncDMLLog", operationSyncDMLProtector);
      new Thread(operationSyncDMLLogService).start();

      // create OperationSyncConsumer
      for (int i = 0; i < config.getOperationSyncConsumerConcurrencySize(); i++) {
        OperationSyncConsumer consumer =
            new OperationSyncConsumer(
                blockingQueue, operationSyncsessionPool, operationSyncDMLLogService);
        new Thread(consumer).start();
      }
    } else {
      operationSyncsessionPool = null;
      operationSyncProducer = null;
      operationSyncDDLProtector = null;
      operationSyncDDLLogService = null;
    }
  }

  @Override
  public QueryContext genQueryContext(
      long queryId, boolean debug, long startTime, String statement, long timeout) {
    return new QueryContext(queryId, debug, startTime, statement, timeout);
  }

  @Override
  public boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    plan.checkIntegrity();
    if (!(plan instanceof SetSystemModePlan)
        && !(plan instanceof FlushPlan)
        && IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineReadonlyException();
    }

    if (isEnableOperationSync) {
      // OperationSync should transmit before execute
      transmitOperationSync(plan);
    }

    return executor.processNonQuery(plan);
  }

  private void transmitOperationSync(PhysicalPlan physicalPlan) {

    OperationSyncPlanTypeUtils.OperationSyncPlanType planType =
        OperationSyncPlanTypeUtils.getOperationSyncPlanType(physicalPlan);
    if (planType == null) {
      // Don't need OperationSync
      return;
    }

    // serialize physical plan
    ByteBuffer buffer;
    try {
      int size = physicalPlan.getSerializedSize();
      ByteArrayOutputStream operationSyncByteStream = new ByteArrayOutputStream(size);
      DataOutputStream operationSyncSerializeStream = new DataOutputStream(operationSyncByteStream);
      physicalPlan.serialize(operationSyncSerializeStream);
      buffer = ByteBuffer.wrap(operationSyncByteStream.toByteArray());
    } catch (IOException e) {
      LOGGER.error("OperationSync can't serialize PhysicalPlan", e);
      return;
    }

    switch (planType) {
      case DDLPlan:
        // Create OperationSyncWriteTask and wait
        OperationSyncWriteTask ddlTask =
            new OperationSyncWriteTask(
                buffer,
                operationSyncsessionPool,
                operationSyncDDLProtector,
                operationSyncDDLLogService);
        ddlTask.run();
        break;
      case DMLPlan:
        // Put into OperationSyncProducer
        operationSyncProducer.put(new Pair<>(buffer, planType));
    }
  }
}
